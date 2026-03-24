from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

CATALOG = spark.conf.get("catalog")
SCHEMA = spark.conf.get("schema")
VOLUME = spark.conf.get("volume")


def raw_table_df(source_subpath: str):
    source_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{source_subpath}"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .load(source_path)
        .withColumnRenamed("value", "raw_json")
        .withColumn("_source_file_path", col("_metadata.file_path"))
        .withColumn("_source_file_name", col("_metadata.file_name"))
        .withColumn("_source_file_size", col("_metadata.file_size"))
        .withColumn("_source_file_modification_time", col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", current_timestamp())
    )


@dp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_flight_status_raw",
    comment="Bronze raw ingestion table for Lufthansa flight status payloads stored as full JSON strings."
)
def bronze_flight_status_raw():
    return raw_table_df("flight_status")


@dp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_schedules_raw",
    comment="Bronze raw ingestion table for Lufthansa schedules payloads stored as full JSON strings."
)
def bronze_schedules_raw():
    return raw_table_df("schedules")