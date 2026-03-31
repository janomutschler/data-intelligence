from pyspark import pipelines as dp
from config import RAW_DATA_SCHEMA, RAW_DATA_VOLUME, BRONZE_SCHEMA
from common import raw_table_df

CATALOG = spark.conf.get("catalog")

BASE_SOURCE_PATH = f"/Volumes/{CATALOG}/{RAW_DATA_SCHEMA}/{RAW_DATA_VOLUME}"

@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA}.flight_status_raw",
    comment="Bronze raw ingestion table for Lufthansa flight status payloads stored as full JSON strings."
)
def bronze_flight_status_raw():
    return raw_table_df(spark, f"{BASE_SOURCE_PATH}/flight_status")

