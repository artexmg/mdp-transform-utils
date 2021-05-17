import time

import pyspark.sql.functions as f
from pyspark.sql import Window

from .constants import TABLES_BY_SCHEMA
from .flatten_structs import flatten_array


DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"


def build_s3_path(bucket, table, load_date):
    return f"{bucket}/TimeSeries/{table}/load_date={load_date}/"


def df_cast_columns_to_timestamp(df, columns, format):
    for column, new_column in columns:
        alias_column = column if not new_column else new_column
        df = df.withColumn(alias_column, f.to_timestamp(f.col(column), format))
        if new_column:
            df = df.drop(column)
    return df


def df_cast_columns_to_date(df, columns, format):
    for column, new_column in columns:
        alias_column = column if not new_column else new_column
        df = df.withColumn(alias_column, f.to_date(f.col(column), format))
        if new_column:
            df = df.drop(column)
    return df


def df_extract_key_from_string(df):
    for column, _ in df.dtypes:
        if not column.endswith("_key") and column not in ["key", "revision", "callid"]:
            df = df.withColumn(column, f.regexp_extract(column, r"(?<=, )(\d{3,})", 1))
    df = df.toDF(*(c.replace("_key", "") for c in df.columns))
    return df


def df_get_max_last_revision(df):
    window = Window.partitionBy("key")
    df_max = (
        df.select("key", "revision")
        .withColumn("max_revision", f.max("revision").over(window))
        .where(f.col("revision") == f.col("max_revision"))
        .drop("max_revision")
    )
    df = df.join(df_max, ["key", "revision"], "inner")
    return df


def get_schema_from_table(table, schema_definition):
    for schema, tables in schema_definition.items():
        if table in tables:
            return schema


def read_data_from_snowflake(spark, sf_options, schemas, table, condition=None):
    table_schema = get_schema_from_table(table, TABLES_BY_SCHEMA)
    sf_options["sfSchema"] = schemas[table_schema]
    df = (
        spark.read.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("query", f"SELECT * FROM {table} {condition}")
        .load()
    )
    df = df.toDF(*[c.lower() for c in df.columns])
    return df


def seconds_to_date(seconds):
    return time.strftime("%H:%M:%S", time.gmtime(seconds))


def eval_case_when_for_date_cols(df, columns):
    for column, new_column in columns:
        alias_column = column if not new_column else new_column
        df = df.withColumn(
            alias_column,
            f.when(f.col(column) == f.lit("1899-12-30 00:00:00").cast("timestamp"), None).otherwise(f.col(column)),
        )
    return df


def get_assignment_engineer_key(df):
    df = df.select(
        "key",
        "revision",
        f.col("engineers.key").alias("engineers_key"),
        f.col("engineers.displaystring").alias("engineers_displaystring"),
    )
    return df


def assignment_split_dates(df):
    df_split = (
        df.select("start_date", "finish_date")
        .dropDuplicates()
        .withColumn("start_tmp", f.explode(f.expr("sequence(start_date, finish_date, interval 1 day)")))
    )
    df2 = df.select("key", "revision", "start_date", "finish_date")
    assignment_split = df2.join(df_split, ["start_date", "finish_date"], how="inner").select(
        "key", "revision", "start_tmp"
    )

    return assignment_split


def get_assignment_by_engineer_date(df):
    columns_to_cast = [("start", None), ("finish", None)]
    df = df.toDF(*(c.replace("stamp_", "") for c in df.columns))
    df = df_cast_columns_to_timestamp(df, columns_to_cast, DATE_FORMAT)
    return df


def assignment_transformation_by_engineer_date(spark, bucket, table, max_revision=True, isna=False):
    df_original = spark.read.orc(f"{bucket}/TimeSeries/{table}")
    print(f"Number of records in Assignment table: {df_original.count()}")

    if isna:
        df_original = df_original.filter(df_original.isna).select("key", "revision", "engineers", "start", "finish")

    df_flattened = flatten_array(df_original)
    df_flattened = flatten_array(df_flattened)
    columns_to_cast = [("start", "start_date"), ("finish", "finish_date")]
    df_flattened = df_cast_columns_to_date(df_flattened, columns_to_cast, DATE_FORMAT)

    assignment_engineerkey = get_assignment_engineer_key(df_flattened)
    assignment_split = assignment_split_dates(df_flattened)
    vw_engineer = get_assignment_by_engineer_date(df_original)

    if max_revision:
        vw_engineer = df_get_max_last_revision(vw_engineer)

    vw_engineer = vw_engineer.withColumn("engineers", f.explode(vw_engineer.engineers)).withColumn(
        "engineers_key", f.col("engineers")["key"]
    )

    vw_engineer = (
        vw_engineer.join(assignment_engineerkey, ["key", "revision", "engineers_key"], how="inner")
        .join(assignment_split, ["key", "revision"], how="inner")
        .drop("engineers", "load_date")
    )

    vw_engineer = (
        vw_engineer.withColumn(
            "start_fixed",
            f.when(
                f.to_date(vw_engineer.start) == vw_engineer.start_tmp,
                vw_engineer.start,
            ).otherwise(f.to_timestamp(f.concat(vw_engineer.start_tmp, f.lit(" 00:00:00")))),
        )
        .withColumn(
            "finish_fixed",
            f.when(
                f.to_date(vw_engineer.finish) == vw_engineer.start_tmp,
                vw_engineer.finish,
            ).otherwise(f.to_timestamp(f.concat(vw_engineer.start_tmp, f.lit(" 23:59:59")))),
        )
        .dropDuplicates()
    )
    return vw_engineer
