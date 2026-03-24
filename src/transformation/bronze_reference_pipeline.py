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
    name=f"{CATALOG}.{SCHEMA}.bronze_airports_raw",
    comment="Bronze raw ingestion table for Lufthansa airport reference payloads stored as full JSON strings."
)
def bronze_airports_raw():
    return raw_table_df("reference_data/airports")


@dp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_airlines_raw",
    comment="Bronze raw ingestion table for Lufthansa airline reference payloads stored as full JSON strings."
)
def bronze_airlines_raw():
    return raw_table_df("reference_data/airlines")


@dp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_aircraft_raw",
    comment="Bronze raw ingestion table for Lufthansa aircraft reference payloads stored as full JSON strings."
)
def bronze_aircraft_raw():
    return raw_table_df("reference_data/aircraft")


@dp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_cities_raw",
    comment="Bronze raw ingestion table for Lufthansa city reference payloads stored as full JSON strings."
)
def bronze_cities_raw():
    return raw_table_df("reference_data/cities")


@dp.table(
    name=f"{CATALOG}.{SCHEMA}.bronze_countries_raw",
    comment="Bronze raw ingestion table for Lufthansa country reference payloads stored as full JSON strings."
)
def bronze_countries_raw():
    return raw_table_df("reference_data/countries")