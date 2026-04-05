from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from config import BRONZE_SCHEMA, SILVER_SCHEMA

CATALOG = spark.conf.get("catalog")

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.airlines_raw"
VALID_SNAPSHOT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.airlines_snapshot_valid"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.airlines_quarantine"
CURRENT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.airlines_current"

IS_INVALID_AIRLINE_ID = F.col("airline_id").isNull() | (F.col("airline_id") == "")
IS_INVALID_AIRLINE_NAME = F.col("airline_name").isNull() | (F.col("airline_name") == "")
IS_INVALID_AIRLINE_ROW = IS_INVALID_AIRLINE_ID | IS_INVALID_AIRLINE_NAME

VALID_AIRLINE_ID_EXPECTATION = "airline_id IS NOT NULL AND TRIM(airline_id) <> ''"
VALID_AIRLINE_NAME_EXPECTATION = "airline_name IS NOT NULL AND TRIM(airline_name) <> ''"

_airline_struct = StructType([
    StructField("AirlineID", StringType(), True),
    StructField("AirlineID_ICAO", StringType(), True),
    StructField("Names", StructType([
        StructField("Name", StructType([
            StructField("@LanguageCode", StringType(), True),
            StructField("$", StringType(), True),
        ]), True)
    ]), True),
])

airlines_array_schema = StructType([
    StructField("AirlineResource", StructType([
        StructField("Airlines", StructType([
            StructField("Airline", ArrayType(_airline_struct), True),
        ]), True)
    ]), True)
])

airlines_single_schema = StructType([
    StructField("AirlineResource", StructType([
        StructField("Airlines", StructType([
            StructField("Airline", _airline_struct, True),
        ]), True)
    ]), True)
])


@dp.temporary_view(name="airlines_latest_run_tmp")
def airlines_latest_run_tmp():
    """
    Filter bronze airlines data to the latest available run_id.
    """
    bronze_df = spark.read.table(BRONZE_TABLE)

    latest_run_df = bronze_df.select(F.max("run_id").alias("run_id"))

    return bronze_df.join(latest_run_df, on="run_id", how="inner")


@dp.temporary_view(name="airlines_exploded_tmp")
def airlines_exploded_tmp():
    """
    Parse raw JSON payload and normalize it into a flat structure.

    Handles both response formats from the API:
    - array of airlines
    - single airline object

    Ensures a consistent array structure and explodes it so each row represents one airline.
    """
    bronze_df = spark.read.table("airlines_latest_run_tmp")

    parsed_array_df = bronze_df.withColumn(
        "parsed_array_json",
        F.from_json(F.col("raw_json"), airlines_array_schema),
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_single_json",
        F.from_json(F.col("raw_json"), airlines_single_schema),
    )

    normalized_df = parsed_both_df.withColumn(
        "airlines_array",
        F.when(
            F.col("parsed_array_json.AirlineResource.Airlines.Airline").isNotNull(),
            F.col("parsed_array_json.AirlineResource.Airlines.Airline"),
        ).otherwise(
            F.array(F.col("parsed_single_json.AirlineResource.Airlines.Airline"))
        ),
    )

    exploded_df = normalized_df.withColumn(
        "airline",
        F.explode_outer(F.col("airlines_array")),
    )

    return exploded_df


@dp.temporary_view(name="airlines_staged_tmp")
def airlines_staged_tmp():
    """
    Select and clean business-relevant fields from exploded airline data.
    This view represents the final staged dataset before validation and further processing.
    """
    exploded_df = spark.read.table("airlines_exploded_tmp")

    staged_df = (
        exploded_df
        .select(
            F.col("airline.AirlineID").alias("airline_id_raw"),
            F.col("airline.AirlineID_ICAO").alias("airline_id_icao_raw"),
            F.col("airline.Names.Name.$").alias("airline_name_raw"),
            "raw_json",
            "run_id",
            F.col("_source_file_modification_time").alias("source_file_modification_ts"),
            F.col("_source_file_path").alias("source_file_path"),
        )
        .withColumn("airline_id", F.upper(F.trim(F.col("airline_id_raw"))))
        .withColumn("airline_id_icao", F.upper(F.trim(F.col("airline_id_icao_raw"))))
        .withColumn("airline_name", F.trim(F.col("airline_name_raw")))
    )

    return staged_df


@dp.materialized_view(
    name=QUARANTINE_TABLE,
    comment="Invalid airline rows from the latest airlines snapshot."
)
def silver_airlines_quarantine():
    """
    Expose invalid airline rows from the latest snapshot for investigation.
    """
    staged_df = spark.read.table("airlines_staged_tmp")

    invalid_df = staged_df.filter(IS_INVALID_AIRLINE_ROW)

    quarantine_df = (
        invalid_df
        .withColumn(
            "validation_error",
            F.when(
                IS_INVALID_AIRLINE_ID,
                F.lit("airline_id missing or invalid"),
            ).otherwise(
                F.lit("airline_name missing or invalid")
            ),
        )
        .withColumn("quarantined_ts", F.current_timestamp())
        .select(
            "airline_id_raw",
            "airline_id_icao_raw",
            "airline_name_raw",
            "validation_error",
            "raw_json",
            "source_file_path",
            "source_file_modification_ts",
            "run_id",
            "quarantined_ts",
        )
    )

    return quarantine_df


@dp.materialized_view(
    name=VALID_SNAPSHOT_TABLE,
    private=True,
    comment="Latest valid airlines snapshot prepared for snapshot-based AUTO CDC."
)
@dp.expect_all_or_drop({
    "valid_airline_id": VALID_AIRLINE_ID_EXPECTATION,
    "valid_airline_name": VALID_AIRLINE_NAME_EXPECTATION,
})
def silver_airlines_snapshot_valid():
    """
    Prepare the latest valid airlines snapshot for downstream processing.

    - Filters out invalid records using expectations
    - Selects relevant business and metadata columns
    - Deduplicates by airline_id to ensure one record per key

    This dataset serves as the clean input snapshot for AUTO CDC (SCD Type 1).
    """
    staged_df = spark.read.table("airlines_staged_tmp")

    valid_df = (
        staged_df
        .select(
            "airline_id",
            "airline_id_icao",
            "airline_name",
            "source_file_path",
            "source_file_modification_ts",
            "run_id",
        )
        .dropDuplicates(["airline_id"])
    )

    return valid_df


dp.create_streaming_table(
    name=CURRENT_TABLE,
    comment="SCD Type 1 current airlines table for Lufthansa reference data."
)

dp.create_auto_cdc_from_snapshot_flow(
    target=CURRENT_TABLE,
    source=VALID_SNAPSHOT_TABLE,
    keys=["airline_id"],
    stored_as_scd_type=1,
)