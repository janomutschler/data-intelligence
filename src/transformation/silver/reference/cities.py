from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from config import BRONZE_SCHEMA, SILVER_SCHEMA

CATALOG = spark.conf.get("catalog")

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.cities_raw"
VALID_SNAPSHOT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.cities_snapshot_valid"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.cities_quarantine"
CURRENT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.cities_current"

IS_INVALID_CITY_CODE = F.col("city_code").isNull() | (F.col("city_code") == "")
IS_INVALID_COUNTRY_CODE = F.col("country_code").isNull() | (F.col("country_code") == "")
IS_INVALID_CITY_NAME = F.col("city_name").isNull() | (F.col("city_name") == "")
IS_INVALID_CITY_ROW = IS_INVALID_CITY_CODE | IS_INVALID_COUNTRY_CODE | IS_INVALID_CITY_NAME

VALID_CITY_CODE_EXPECTATION = "city_code IS NOT NULL AND TRIM(city_code) <> ''"
VALID_COUNTRY_CODE_EXPECTATION = "country_code IS NOT NULL AND TRIM(country_code) <> ''"
VALID_CITY_NAME_EXPECTATION = "city_name IS NOT NULL AND TRIM(city_name) <> ''"

cities_array_schema = StructType([
    StructField("CityResource", StructType([
        StructField("Cities", StructType([
            StructField(
                "City",
                ArrayType(
                    StructType([
                        StructField("CityCode", StringType(), True),
                        StructField("CountryCode", StringType(), True),
                        StructField("Names", StructType([
                            StructField("Name", StructType([
                                StructField("@LanguageCode", StringType(), True),
                                StructField("$", StringType(), True),
                            ]), True)
                        ]), True),
                        StructField("UtcOffset", StringType(), True),
                        StructField("TimeZoneId", StringType(), True),
                        StructField("Airports", StructType([
                            StructField("AirportCode", StringType(), True)
                        ]), True),
                    ])
                ),
                True,
            )
        ]), True)
    ]), True)
])

cities_single_schema = StructType([
    StructField("CityResource", StructType([
        StructField("Cities", StructType([
            StructField(
                "City",
                StructType([
                    StructField("CityCode", StringType(), True),
                    StructField("CountryCode", StringType(), True),
                    StructField("Names", StructType([
                        StructField("Name", StructType([
                            StructField("@LanguageCode", StringType(), True),
                            StructField("$", StringType(), True),
                        ]), True)
                    ]), True),
                    StructField("UtcOffset", StringType(), True),
                    StructField("TimeZoneId", StringType(), True),
                    StructField("Airports", StructType([
                        StructField("AirportCode", StringType(), True)
                    ]), True),
                ]),
                True,
            )
        ]), True)
    ]), True)
])


@dp.temporary_view(name="cities_latest_run_tmp")
def cities_latest_run_tmp():
    """
    Filter bronze cities data to the latest available run_id.
    """
    bronze_df = spark.read.table(BRONZE_TABLE)

    latest_run_df = bronze_df.select(F.max("run_id").alias("run_id"))

    return bronze_df.join(latest_run_df, on="run_id", how="inner")


@dp.temporary_view(name="cities_exploded_tmp")
def cities_exploded_tmp():
    """
    Parse raw JSON payload and normalize it into a flat structure.

    Handles both response formats from the API:
    - array of cities
    - single city object

    Ensures a consistent array structure and explodes it so each row represents one city.
    """
    bronze_df = spark.read.table("cities_latest_run_tmp")

    parsed_array_df = bronze_df.withColumn(
        "parsed_array_json",
        F.from_json(F.col("raw_json"), cities_array_schema),
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_single_json",
        F.from_json(F.col("raw_json"), cities_single_schema),
    )

    normalized_df = parsed_both_df.withColumn(
        "cities_array",
        F.when(
            F.col("parsed_array_json.CityResource.Cities.City").isNotNull(),
            F.col("parsed_array_json.CityResource.Cities.City"),
        ).otherwise(
            F.array(F.col("parsed_single_json.CityResource.Cities.City"))
        ),
    )

    exploded_df = normalized_df.withColumn(
        "city",
        F.explode_outer(F.col("cities_array")),
    )

    return exploded_df


@dp.temporary_view(name="cities_staged_tmp")
def cities_staged_tmp():
    """
    Select and clean business-relevant fields from exploded city data.
    This view represents the final staged dataset before validation and further processing.
    """
    exploded_df = spark.read.table("cities_exploded_tmp")

    staged_df = (
        exploded_df
        .select(
            F.col("city.CityCode").alias("city_code_raw"),
            F.col("city.CountryCode").alias("country_code_raw"),
            F.col("city.Names.Name.$").alias("city_name_raw"),
            F.col("city.UtcOffset").alias("utc_offset_raw"),
            F.col("city.TimeZoneId").alias("time_zone_id_raw"),
            "raw_json",
            "run_id",
            F.col("_source_file_modification_time").alias("source_file_modification_ts"),
            F.col("_source_file_path").alias("source_file_path"),
        )
        .withColumn("city_code", F.upper(F.trim(F.col("city_code_raw"))))
        .withColumn("country_code", F.upper(F.trim(F.col("country_code_raw"))))
        .withColumn("city_name", F.trim(F.col("city_name_raw")))
        .withColumn("utc_offset", F.trim(F.col("utc_offset_raw")))
        .withColumn("time_zone_id", F.trim(F.col("time_zone_id_raw")))
    )

    return staged_df


@dp.materialized_view(
    name=QUARANTINE_TABLE,
    comment="Invalid city rows from the latest cities snapshot."
)
def silver_cities_quarantine():
    """
    Expose invalid city rows from the latest snapshot for investigation.
    """
    staged_df = spark.read.table("cities_staged_tmp")

    invalid_df = staged_df.filter(IS_INVALID_CITY_ROW)

    quarantine_df = (
        invalid_df
        .withColumn(
            "validation_error",
            F.when(
                IS_INVALID_CITY_CODE,
                F.lit("city_code missing or invalid"),
            ).when(
                IS_INVALID_COUNTRY_CODE,
                F.lit("country_code missing or invalid"),
            ).otherwise(
                F.lit("city_name missing or invalid")
            ),
        )
        .withColumn("quarantined_ts", F.current_timestamp())
        .select(
            "city_code_raw",
            "country_code_raw",
            "city_name_raw",
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
    comment="Latest valid cities snapshot prepared for snapshot-based AUTO CDC."
)
@dp.expect_all_or_drop({
    "valid_city_code": VALID_CITY_CODE_EXPECTATION,
    "valid_country_code": VALID_COUNTRY_CODE_EXPECTATION,
    "valid_city_name": VALID_CITY_NAME_EXPECTATION,
})
def silver_cities_snapshot_valid():
    """
    Prepare the latest valid cities snapshot for downstream processing.

    - Filters out invalid records using expectations
    - Selects relevant business and metadata columns
    - Deduplicates by city_code and country_code to ensure one record per key

    This dataset serves as the clean input snapshot for AUTO CDC (SCD Type 1).
    """
    staged_df = spark.read.table("cities_staged_tmp")

    valid_df = (
        staged_df
        .select(
            "city_code",
            "country_code",
            "city_name",
            "utc_offset",
            "time_zone_id",
            "source_file_path",
            "source_file_modification_ts",
            "run_id",
        )
        .dropDuplicates(["city_code", "country_code"])
    )

    return valid_df


dp.create_streaming_table(
    name=CURRENT_TABLE,
    comment="SCD Type 1 current cities table for Lufthansa reference data."
)

dp.create_auto_cdc_from_snapshot_flow(
    target=CURRENT_TABLE,
    source=VALID_SNAPSHOT_TABLE,
    keys=["city_code", "country_code"],
    stored_as_scd_type=1,
)