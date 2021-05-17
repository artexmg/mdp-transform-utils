import pyspark
import pyspark.sql.functions as f


def is_df_flat(df):
    for (_, dtype) in df.dtypes:
        if ("array" in dtype) or ("struct" in dtype):
            return False
    return True


def flatten_array(df):
    for column, column_type in df.dtypes:
        if column_type.startswith("array<"):
            df = df.select("*", f.explode_outer(df[column]).alias(f"{column}_2"))
            df = df.drop(column)
    df = df.toDF(*(c.replace("_2", "") for c in df.columns))
    return df.select("*")


def schema_to_columns(schema):
    columns = list()

    def helper(schm, prefix=None):
        if prefix is None:
            prefix = list()
        for item in schm.fields:
            if isinstance(item.dataType, pyspark.sql.types.StructType):
                helper(item.dataType, prefix + [item.name])
            else:
                columns.append(prefix + [item.name])

    helper(schema)
    return columns


def flatten_df(df):
    aliased_columns = list()

    for col_spec in schema_to_columns(df.schema):
        if len(col_spec) == 1:
            aliased_columns.append(col_spec[0])
        else:
            aliased_columns.append(df[".".join(col_spec)].alias("_".join(col_spec)))
    return df.select(aliased_columns)
