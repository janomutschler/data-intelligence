from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from config import BRONZE_SCHEMA, SILVER_SCHEMA

CATALOG = spark.conf.get("catalog")

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.aircraft_raw"
VALID_SNAPSHOT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.aircraft_snapshot_valid"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.aircraft_quarantine"
CURRENT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.aircraft_current"

IS_INVALID_AIRCRAFT_CODE = F.col("aircraft_code").isNull() | (F.col("aircraft_code") == "")
IS_INVALID_AIRCRAFT_NAME = F.col("aircraft_name").isNull() | (F.col("aircraft_name") == "")
IS_INVALID_AIRCRAFT_ROW = IS_INVALID_AIRCRAFT_CODE | IS_INVALID_AIRCRAFT_NAME

VALID_AIRCRAFT_CODE_EXPECTATION = "aircraft_code IS NOT NULL AND TRIM(aircraft_code) <> ''"
VALID_AIRCRAFT_NAME_EXPECTATION = "aircraft_name IS NOT NULL AND TRIM(aircraft_name) <> ''"

_aircraft_struct = StructType([
    StructField("AircraftCode", StringType(), True),
    StructField("Names", StructType([
        StructField("Name", StructType([
            StructField("@LanguageCode", StringType(), True),
            StructField("$", StringType(), True),
        ]), True)
    ]), True),
    StructField("AirlineEquipCode", StringType(), True),
])

aircraft_array_schema = StructType([
    StructField("AircraftResource", StructType([
        StructField("AircraftSummaries", StructType([
            StructField("AircraftSummary", ArrayType(_aircraft_struct), True),
        ]), True)
    ]), True)
])

aircraft_single_schema = StructType([
    StructField("AircraftResource", StructType([
        StructField("AircraftSummaries", StructType([
            StructField("AircraftSummary", _aircraft_struct, True),
        ]), True)
    ]), True)
])


@dp.temporary_view(name="aircraft_latest_run_tmp")
def aircraft_latest_run_tmp():
    """
    Filter bronze aircraft data to the latest available run_id.
    """
    bronze_df = spark.read.table(BRONZE_TABLE)

    latest_run_df = bronze_df.select(F.max("run_id").alias("run_id"))

    return bronze_df.join(latest_run_df, on="run_id", how="inner")


@dp.temporary_view(name="aircraft_exploded_tmp")
def aircraft_exploded_tmp():
    """
    Parse raw JSON payload and normalize it into a flat structure.

    Handles both response formats from the API:
    - array of aircraft
    - single aircraft object

    Ensures a consistent array structure and explodes it so each row represents one aircraft.
    """
    bronze_df = spark.read.table("aircraft_latest_run_tmp")

    parsed_array_df = bronze_df.withColumn(
        "parsed_array_json",
        F.from_json(F.col("raw_json"), aircraft_array_schema),
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_single_json",
        F.from_json(F.col("raw_json"), aircraft_single_schema),
    )

    normalized_df = parsed_both_df.withColumn(
        "aircraft_array",
        F.when(
            F.col("parsed_array_json.AircraftResource.AircraftSummaries.AircraftSummary").isNotNull(),
            F.col("parsed_array_json.AircraftResource.AircraftSummaries.AircraftSummary"),
        ).otherwise(
            F.array(F.col("parsed_single_json.AircraftResource.AircraftSummaries.AircraftSummary"))
        ),
    )

    exploded_df = normalized_df.withColumn(
        "aircraft",
        F.explode_outer(F.col("aircraft_array")),
    )

    return exploded_df


@dp.temporary_view(name="aircraft_staged_tmp")
def aircraft_staged_tmp():
    """
    Select and clean business-relevant fields from exploded aircraft data.
    This view represents the final staged dataset before validation and further processing.
    """
    exploded_df = spark.read.table("aircraft_exploded_tmp")

    staged_df = (
        exploded_df
        .select(
            F.col("aircraft.AircraftCode").alias("aircraft_code_raw"),
            F.col("aircraft.Names.Name.$").alias("aircraft_name_raw"),
            F.col("aircraft.AirlineEquipCode").alias("airline_equip_code_raw"),
            "raw_json",
            "run_id",
            F.col("_source_file_modification_time").alias("source_file_modification_ts"),
            F.col("_source_file_path").alias("source_file_path"),
        )
        .withColumn("aircraft_code", F.upper(F.trim(F.col("aircraft_code_raw"))))
        .withColumn("aircraft_name", F.trim(F.col("aircraft_name_raw")))
        .withColumn("airline_equip_code", F.upper(F.trim(F.col("airline_equip_code_raw"))))
    )

    return staged_df


@dp.materialized_view(
    name=QUARANTINE_TABLE,
    comment="Invalid aircraft rows from the latest aircraft snapshot."
)
def silver_aircraft_quarantine():
    """
    Expose invalid aircraft rows from the latest snapshot for investigation.
    """
    staged_df = spark.read.table("aircraft_staged_tmp")

    invalid_df = staged_df.filter(IS_INVALID_AIRCRAFT_ROW)

    quarantine_df = (
        invalid_df
        .withColumn(
            "validation_error",
            F.when(
                IS_INVALID_AIRCRAFT_CODE,
                F.lit("aircraft_code missing or invalid"),
            ).otherwise(
                F.lit("aircraft_name missing or invalid")
            ),
        )
        .withColumn("quarantined_ts", F.current_timestamp())
        .select(
            "aircraft_code_raw",
            "aircraft_name_raw",
            "airline_equip_code_raw",
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
    comment="Latest valid aircraft snapshot prepared for snapshot-based AUTO CDC."
)
@dp.expect_all_or_drop({
    "valid_aircraft_code": VALID_AIRCRAFT_CODE_EXPECTATION,
    "valid_aircraft_name": VALID_AIRCRAFT_NAME_EXPECTATION,
})
def silver_aircraft_snapshot_valid():
    """
    Prepare the latest valid aircraft snapshot for downstream processing.

    - Filters out invalid records using expectations
    - Selects relevant business and metadata columns
    - Deduplicates by aircraft_code to ensure one record per key

    This dataset serves as the clean input snapshot for AUTO CDC (SCD Type 1).
    """
    staged_df = spark.read.table("aircraft_staged_tmp")

    valid_df = (
        staged_df
        .select(
            "aircraft_code",
            "aircraft_name",
            "airline_equip_code",
            "source_file_path",
            "source_file_modification_ts",
            "run_id",
        )
        .dropDuplicates(["aircraft_code"])
    )

    return valid_df


dp.create_streaming_table(
    name=CURRENT_TABLE,
    comment="SCD Type 1 current aircraft table for Lufthansa reference data."
)

dp.create_auto_cdc_from_snapshot_flow(
    target=CURRENT_TABLE,
    source=VALID_SNAPSHOT_TABLE,
    keys=["aircraft_code"],
    stored_as_scd_type=1,
)