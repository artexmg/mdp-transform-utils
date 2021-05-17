import logging
import os
import re
import urllib.parse
from datetime import datetime, timedelta
from logging import Logger

import boto3
from pyspark.sql import DataFrame, DataFrameWriter
from pyspark.sql.functions import col, lit, to_date


def get_ssm_param(ssm_client, name):
    return ssm_client.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]


# change datatype of `date` type column into `timestamp`
def change_dtype(df: DataFrame):
    for name, dtype in df.dtypes:
        if dtype == "date":
            df = df.withColumn(name, col(name).cast("timestamp"))
    return df


def unload_to_snowflake(df, sf_options, table):
    logging.info(f"Loading {table} table to Snowflake")
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", table)
        .option("truncate_table", "on")
        .mode("overwrite")
        .save()
    )
    logging.info(f"{table} Snowflake table has been loaded successfully")


def configure_logging(logger: Logger):
    logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
    handler = logging.StreamHandler()
    bf = logging.Formatter("[{asctime}:{funcName}:{levelname:8s}] {message}", style="{")
    handler.setFormatter(bf)
    logger.addHandler(handler)


def get_bucket_from_url(url):
    return urllib.parse.urlparse(url, allow_fragments=False).netloc


def read_csv(s3, s3_path):
    bucket = get_bucket_from_url(s3_path)
    path = s3_path.split("/", maxsplit=3)[-1]
    source_objects = s3.Bucket(bucket).Object(key=path)
    data = source_objects.get()
    csv = data["Body"].read().decode("utf-8").split()
    return csv


def date_to_str(date, format="long"):
    return date.strftime("%Y-%m-%d") if format == "short" else date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-4]


def is_s3_folder_empty(bucket, key, aws_region):
    s3 = boto3.resource("s3", region_name=aws_region)
    bucket = s3.Bucket(bucket)
    objs = list(bucket.objects.filter(Prefix=key))
    return not len(objs) >= 1


def camel_to_snake(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def with_load_date_column(df, load_date):
    return df.withColumn("load_date", to_date(lit(load_date)))


def partitioned_by_load_date(df_writer) -> DataFrameWriter:
    return df_writer.partitionBy("load_date")


def build_s3_path(bucket, table, load_date=None):
    type_of_request = "NonTimeSeries"
    if load_date:
        type_of_request = "TimeSeries"
    return f"{bucket}/{type_of_request}/{table}"


def verify_response(response):
    return True if response.ok else False


def halve_date_range(start, end, split):
    total_seconds = int((end - start).total_seconds())
    delta = total_seconds / split
    starts = [start + timedelta(seconds=delta * i) for i in range(split)]
    ends = [s + timedelta(seconds=delta - 1) for s in starts]
    ends[len(ends) - 1] = end
    return zip(starts, ends)


def build_endpoint_url(base_url, table, start_dt=None, end_dt=None):
    url = f"{base_url}/objects/{table}"
    if start_dt and end_dt:
        print(f"Building endpoint url with date range [{start_dt} - {end_dt}]")
        start_dt = date_to_str(start_dt)
        end_dt = date_to_str(end_dt)
        query_filter = f"Stamp/TimeModified gt {start_dt}Z and Stamp/TimeModified lt {end_dt}Z"
        url = f"{url}/?$filter={urllib.parse.quote(query_filter)}"
    return url


def write_to_snowflake(df, sf_options, table):
    df.write.format("net.snowflake.spark.snowflake").options(**sf_options).option("dbtable", table).mode(
        "overwrite"
    ).save()


def write_to_snowflake_history(df, sf_options, table, load_date):
    table = table.upper()
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", f'"{table}"')
        .option("preactions", f"DELETE FROM \"{table}\" WHERE LOAD_DATE = to_date('{load_date}')")
        .option("column_mapping", "name")
        .mode("append")
        .save()
    )


def get_backfill_dates(start_date, end_date):
    dates = []
    backfill_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    backfill_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    delta = backfill_end_date - backfill_start_date
    for i in range(delta.days + 1):
        day = backfill_start_date + timedelta(days=i)
        day = day.strftime("%Y-%m-%d")
        dates.append(day)
    return dates

def amg(start, end):
    f = lambda x : datetime.strptime(x,"%Y-%m-%d").date() 
    return [ (f(start)+timedelta(d)).strftime("%Y-%m-%d") for d in range((f(end)-f(start)).days+1)]