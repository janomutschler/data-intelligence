from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    DoubleType,
)
from config import BRONZE_SCHEMA, SILVER_SCHEMA

CATALOG = spark.conf.get("catalog")

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.airports_raw"
VALID_SNAPSHOT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.airports_snapshot_valid"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.airports_quarantine"
CURRENT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.airports_current"

IS_INVALID_AIRPORT_CODE = F.col("airport_code").isNull() | (F.col("airport_code") == "")
IS_INVALID_AIRPORT_NAME = F.col("airport_name").isNull() | (F.col("airport_name") == "")
IS_INVALID_CITY_CODE = F.col("city_code").isNull() | (F.col("city_code") == "")
IS_INVALID_COUNTRY_CODE = F.col("country_code").isNull() | (F.col("country_code") == "")
IS_INVALID_AIRPORT_ROW = (
    IS_INVALID_AIRPORT_CODE |
    IS_INVALID_AIRPORT_NAME |
    IS_INVALID_CITY_CODE |
    IS_INVALID_COUNTRY_CODE
)

VALID_AIRPORT_CODE_EXPECTATION = "airport_code IS NOT NULL AND TRIM(airport_code) <> ''"
VALID_AIRPORT_NAME_EXPECTATION = "airport_name IS NOT NULL AND TRIM(airport_name) <> ''"
VALID_CITY_CODE_EXPECTATION = "city_code IS NOT NULL AND TRIM(city_code) <> ''"
VALID_COUNTRY_CODE_EXPECTATION = "country_code IS NOT NULL AND TRIM(country_code) <> ''"

_airport_struct = StructType([
    StructField("AirportCode", StringType(), True),
    StructField("Position", StructType([
        StructField("Coordinate", StructType([
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True),
        ]), True)
    ]), True),
    StructField("CityCode", StringType(), True),
    StructField("CountryCode", StringType(), True),
    StructField("LocationType", StringType(), True),
    StructField("Names", StructType([
        StructField("Name", StructType([
            StructField("@LanguageCode", StringType(), True),
            StructField("$", StringType(), True),
        ]), True)
    ]), True),
    StructField("UtcOffset", StringType(), True),
    StructField("TimeZoneId", StringType(), True),
])

airports_array_schema = StructType([
    StructField("AirportResource", StructType([
        StructField("Airports", StructType([
            StructField("Airport", ArrayType(_airport_struct), True),
        ]), True)
    ]), True)
])

airports_single_schema = StructType([
    StructField("AirportResource", StructType([
        StructField("Airports", StructType([
            StructField("Airport", _airport_struct, True),
        ]), True)
    ]), True)
])


@dp.temporary_view(name="airports_latest_run_tmp")
def airports_latest_run_tmp():
    """
    Filter bronze airports data to the latest available run_id.
    """
    bronze_df = spark.read.table(BRONZE_TABLE)

    latest_run_df = bronze_df.select(F.max("run_id").alias("run_id"))

    return bronze_df.join(latest_run_df, on="run_id", how="inner")


@dp.temporary_view(name="airports_exploded_tmp")
def airports_exploded_tmp():
    """
    Parse raw JSON payload and normalize it into a flat structure.

    Handles both response formats from the API:
    - array of airports
    - single airport object

    Ensures a consistent array structure and explodes it so each row represents one airport.
    """
    bronze_df = spark.read.table("airports_latest_run_tmp")

    parsed_array_df = bronze_df.withColumn(
        "parsed_array_json",
        F.from_json(F.col("raw_json"), airports_array_schema),
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_single_json",
        F.from_json(F.col("raw_json"), airports_single_schema),
    )

    normalized_df = parsed_both_df.withColumn(
        "airports_array",
        F.when(
            F.col("parsed_array_json.AirportResource.Airports.Airport").isNotNull(),
            F.col("parsed_array_json.AirportResource.Airports.Airport"),
        ).otherwise(
            F.array(F.col("parsed_single_json.AirportResource.Airports.Airport"))
        ),
    )

    exploded_df = normalized_df.withColumn(
        "airport",
        F.explode_outer(F.col("airports_array")),
    )

    return exploded_df


@dp.temporary_view(name="airports_staged_tmp")
def airports_staged_tmp():
    """
    Select and clean business-relevant fields from exploded airport data.
    This view represents the final staged dataset before validation and further processing.
    """
    exploded_df = spark.read.table("airports_exploded_tmp")

    staged_df = (
        exploded_df
        .select(
            F.col("airport.AirportCode").alias("airport_code_raw"),
            F.col("airport.Names.Name.$").alias("airport_name_raw"),
            F.col("airport.CityCode").alias("city_code_raw"),
            F.col("airport.CountryCode").alias("country_code_raw"),
            F.col("airport.Position.Coordinate.Latitude").alias("latitude_raw"),
            F.col("airport.Position.Coordinate.Longitude").alias("longitude_raw"),
            F.col("airport.LocationType").alias("location_type_raw"),
            F.col("airport.UtcOffset").alias("utc_offset_raw"),
            F.col("airport.TimeZoneId").alias("time_zone_id_raw"),
            "raw_json",
            "run_id",
            F.col("_source_file_modification_time").alias("source_file_modification_ts"),
            F.col("_source_file_path").alias("source_file_path"),
        )
        .withColumn("airport_code", F.upper(F.trim(F.col("airport_code_raw"))))
        .withColumn("airport_name", F.trim(F.col("airport_name_raw")))
        .withColumn("city_code", F.upper(F.trim(F.col("city_code_raw"))))
        .withColumn("country_code", F.upper(F.trim(F.col("country_code_raw"))))
        .withColumn("latitude", F.col("latitude_raw"))
        .withColumn("longitude", F.col("longitude_raw"))
        .withColumn("location_type", F.trim(F.col("location_type_raw")))
        .withColumn("utc_offset", F.trim(F.col("utc_offset_raw")))
        .withColumn("time_zone_id", F.trim(F.col("time_zone_id_raw")))
    )

    return staged_df


@dp.materialized_view(
    name=QUARANTINE_TABLE,
    comment="Invalid airport rows from the latest airports snapshot."
)
def silver_airports_quarantine():
    """
    Expose invalid airport rows from the latest snapshot for investigation.
    """
    staged_df = spark.read.table("airports_staged_tmp")

    invalid_df = staged_df.filter(IS_INVALID_AIRPORT_ROW)

    quarantine_df = (
        invalid_df
        .withColumn(
            "validation_error",
            F.when(
                IS_INVALID_AIRPORT_CODE,
                F.lit("airport_code missing or invalid"),
            ).when(
                IS_INVALID_AIRPORT_NAME,
                F.lit("airport_name missing or invalid"),
            ).when(
                IS_INVALID_CITY_CODE,
                F.lit("city_code missing or invalid"),
            ).otherwise(
                F.lit("country_code missing or invalid")
            ),
        )
        .withColumn("quarantined_ts", F.current_timestamp())
        .select(
            "airport_code_raw",
            "airport_name_raw",
            "city_code_raw",
            "country_code_raw",
            "latitude_raw",
            "longitude_raw",
            "location_type_raw",
            "utc_offset_raw",
            "time_zone_id_raw",
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
    comment="Latest valid airports snapshot prepared for snapshot-based AUTO CDC."
)
@dp.expect_all_or_drop({
    "valid_airport_code": VALID_AIRPORT_CODE_EXPECTATION,
    "valid_airport_name": VALID_AIRPORT_NAME_EXPECTATION,
    "valid_city_code": VALID_CITY_CODE_EXPECTATION,
    "valid_country_code": VALID_COUNTRY_CODE_EXPECTATION,
})
def silver_airports_snapshot_valid():
    """
    Prepare the latest valid airports snapshot for downstream processing.

    - Filters out invalid records using expectations
    - Selects relevant business and metadata columns
    - Deduplicates by airport_code to ensure one record per key

    This dataset serves as the clean input snapshot for AUTO CDC (SCD Type 1).
    """
    staged_df = spark.read.table("airports_staged_tmp")

    valid_df = (
        staged_df
        .select(
            "airport_code",
            "airport_name",
            "city_code",
            "country_code",
            "latitude",
            "longitude",
            "location_type",
            "utc_offset",
            "time_zone_id",
            "source_file_path",
            "source_file_modification_ts",
            "run_id",
        )
        .dropDuplicates(["airport_code"])
    )

    return valid_df


dp.create_streaming_table(
    name=CURRENT_TABLE,
    comment="SCD Type 1 current airports table for Lufthansa reference data."
)

dp.create_auto_cdc_from_snapshot_flow(
    target=CURRENT_TABLE,
    source=VALID_SNAPSHOT_TABLE,
    keys=["airport_code"],
    stored_as_scd_type=1,
)