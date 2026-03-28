import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from config import CATALOG, SCHEMA


FLIGHT_STATUS_LOG_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_log_flight_status"
REFERENCE_DATA_LOG_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_log_reference_data"
SCHEDULES_LOG_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_log_schedules"


flight_status_log_schema = StructType([
    StructField("log_date", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("status", StringType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("attempt", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("response_text", StringType(), True),
    StructField("exception", StringType(), True),
    StructField("airport", StringType(), True),
    StructField("flight_date", StringType(), True),
    StructField("window_start", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("page", IntegerType(), True),
    StructField("offset", IntegerType(), True),
    StructField("file_path", StringType(), True),
    StructField("records_total", IntegerType(), True),
    StructField("params_json", StringType(), True),
])


reference_data_log_schema = StructType([
    StructField("log_date", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("status", StringType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("attempt", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("response_text", StringType(), True),
    StructField("exception", StringType(), True),
    StructField("reference_type", StringType(), True),
    StructField("reference_date", StringType(), True),
    StructField("page", IntegerType(), True),
    StructField("offset", IntegerType(), True),
    StructField("file_path", StringType(), True),
    StructField("records_total", IntegerType(), True),
    StructField("params_json", StringType(), True),
])


def create_flight_status_log_table(spark) -> None:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FLIGHT_STATUS_LOG_TABLE} (
        log_date STRING,
        run_id STRING,
        timestamp_utc STRING,
        status STRING,
        http_status INT,
        attempt INT,
        url STRING,
        response_text STRING,
        exception STRING,
        airport STRING,
        flight_date STRING,
        window_start STRING,
        direction STRING,
        page INT,
        offset INT,
        file_path STRING,
        records_total INT,
        params_json STRING
    )
    USING DELTA
    PARTITIONED BY (log_date)
    """)


def create_reference_data_log_table(spark) -> None:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {REFERENCE_DATA_LOG_TABLE} (
        log_date STRING,
        run_id STRING,
        timestamp_utc STRING,
        status STRING,
        http_status INT,
        attempt INT,
        url STRING,
        response_text STRING,
        exception STRING,
        reference_type STRING,
        reference_date STRING,
        page INT,
        offset INT,
        file_path STRING,
        records_total INT,
        params_json STRING
    )
    USING DELTA
    PARTITIONED BY (log_date)
    """)



def _append_log(spark, table_name: str, schema: StructType, run_id: str, record: dict) -> None:
    """
    saves log in normalized format to table
    """
    normalized_record = {
        **record,
        "run_id": run_id,
        "log_date": record.get("log_date") or record.get("timestamp_utc", "")[:10] or None,
        "params_json": json.dumps(record.get("params")) if record.get("params") is not None else None,
    }

    df = spark.createDataFrame([normalized_record], schema=schema)
    df.write.format("delta").mode("append").saveAsTable(table_name)


def append_flight_status_log(spark, run_id: str, record: dict) -> None:
    _append_log(spark, FLIGHT_STATUS_LOG_TABLE, flight_status_log_schema, run_id, record)


def append_reference_data_log(spark, run_id: str, record: dict) -> None:
    _append_log(spark, REFERENCE_DATA_LOG_TABLE, reference_data_log_schema, run_id, record)

