from pyspark import pipelines as dp
from config import RAW_DATA_SCHEMA, RAW_DATA_VOLUME, BRONZE_SCHEMA
from common import raw_table_df

CATALOG = spark.conf.get("catalog")

BASE_SOURCE_PATH = f"/Volumes/{CATALOG}/{RAW_DATA_SCHEMA}/{RAW_DATA_VOLUME}"


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.airports_raw",
    comment="Bronze raw ingestion table for Lufthansa airport reference payloads stored as full JSON strings."
)
def bronze_airports_raw():
    return raw_table_df(spark, f"{BASE_SOURCE_PATH}/reference_data/airports")


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.airlines_raw",
    comment="Bronze raw ingestion table for Lufthansa airline reference payloads stored as full JSON strings."
)
def bronze_airlines_raw():
    return raw_table_df(spark, f"{BASE_SOURCE_PATH}/reference_data/airlines")


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.aircraft_raw",
    comment="Bronze raw ingestion table for Lufthansa aircraft reference payloads stored as full JSON strings."
)
def bronze_aircraft_raw():
    return raw_table_df(spark, f"{BASE_SOURCE_PATH}/reference_data/aircraft")


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.cities_raw",
    comment="Bronze raw ingestion table for Lufthansa city reference payloads stored as full JSON strings."
)
def bronze_cities_raw():
    return raw_table_df(spark, f"{BASE_SOURCE_PATH}/reference_data/cities")


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.countries_raw",
    comment="Bronze raw ingestion table for Lufthansa country reference payloads stored as full JSON strings."
)
def bronze_countries_raw():
    return raw_table_df(spark, f"{BASE_SOURCE_PATH}/reference_data/countries")