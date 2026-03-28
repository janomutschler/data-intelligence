from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from config import RAW_DATA_SCHEMA, RAW_DATA_VOLUME, BRONZE_SCHEMA

CATALOG = spark.conf.get("catalog")
BRONZE_SCHEMA = "bronze"
RAW_DATA_SCHEMA = "raw_data"
RAW_DATA_VOLUME = "raw_lh_data"


def raw_table_df(source_subpath: str):
    source_path = f"/Volumes/{CATALOG}/{RAW_DATA_SCHEMA}/{RAW_DATA_VOLUME}/{source_subpath}"

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
    name=f"{CATALOG}.{BRONZE_SCHEMA}.bronze_flight_status_raw",
    comment="Bronze raw ingestion table for Lufthansa flight status payloads stored as full JSON strings."
)
def bronze_flight_status_raw():
    return raw_table_df("flight_status")

