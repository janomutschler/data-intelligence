from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from config import BRONZE_SCHEMA, SILVER_SCHEMA

CATALOG = spark.conf.get("catalog")

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_countries_raw"
VALID_SNAPSHOT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_countries_snapshot_valid"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_countries_quarantine"
CURRENT_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_countries_current"

IS_INVALID_COUNTRY_CODE = F.col("country_code").isNull() | (F.col("country_code") == "")
IS_INVALID_COUNTRY_NAME = F.col("country_name").isNull() | (F.col("country_name") == "")
IS_INVALID_COUNTRY_ROW = IS_INVALID_COUNTRY_CODE | IS_INVALID_COUNTRY_NAME

VALID_COUNTRY_CODE_EXPECTATION = "country_code IS NOT NULL AND TRIM(country_code) <> ''"
VALID_COUNTRY_NAME_EXPECTATION = "country_name IS NOT NULL AND TRIM(country_name) <> ''"

countries_array_schema = StructType([
    StructField("CountryResource", StructType([
        StructField("Countries", StructType([
            StructField(
                "Country",
                ArrayType(
                    StructType([
                        StructField("CountryCode", StringType(), True),
                        StructField("Names", StructType([
                            StructField("Name", StructType([
                                StructField("@LanguageCode", StringType(), True),
                                StructField("$", StringType(), True),
                            ]), True)
                        ]), True),
                    ])
                ),
                True,
            )
        ]), True)
    ]), True)
])

countries_single_schema = StructType([
    StructField("CountryResource", StructType([
        StructField("Countries", StructType([
            StructField(
                "Country",
                StructType([
                    StructField("CountryCode", StringType(), True),
                    StructField("Names", StructType([
                        StructField("Name", StructType([
                            StructField("@LanguageCode", StringType(), True),
                            StructField("$", StringType(), True),
                        ]), True)
                    ]), True),
                ]),
                True,
            )
        ]), True)
    ]), True)
])


@dp.temporary_view(name="countries_latest_run_tmp")
def countries_latest_run_tmp():
    """
    Filter bronze countries data to the latest available run_id.
    """
    bronze_df = spark.read.table(BRONZE_TABLE)

    latest_run_df = bronze_df.select(F.max("run_id").alias("run_id"))

    return bronze_df.join(latest_run_df, on="run_id", how="inner")


@dp.temporary_view(name="countries_exploded_tmp")
def countries_exploded_tmp():
    """
    Parse raw JSON payload and normalize it into a flat structure.

    Handles both response formats from the API:
    - array of countries
    - single country object

    Ensures a consistent array structure and explodes it so each row represents one country.
    """
    bronze_df = spark.read.table("countries_latest_run_tmp")

    parsed_array_df = bronze_df.withColumn(
        "parsed_array_json",
        F.from_json(F.col("raw_json"), countries_array_schema),
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_single_json",
        F.from_json(F.col("raw_json"), countries_single_schema),
    )

    normalized_df = parsed_both_df.withColumn(
        "countries_array",
        F.when(
            F.col("parsed_array_json.CountryResource.Countries.Country").isNotNull(),
            F.col("parsed_array_json.CountryResource.Countries.Country"),
        ).otherwise(
            F.array(F.col("parsed_single_json.CountryResource.Countries.Country"))
        ),
    )

    exploded_df = normalized_df.withColumn(
        "country",
        F.explode_outer(F.col("countries_array")),
    )

    return exploded_df


@dp.temporary_view(name="countries_staged_tmp")
def countries_staged_tmp():
    """
    Select and clean business-relevant fields from exploded country data.
    This view represents the final staged dataset before validation and further processing.
    """
    exploded_df = spark.read.table("countries_exploded_tmp")

    staged_df = (
        exploded_df
        .select(
            F.col("country.CountryCode").alias("country_code_raw"),
            F.col("country.Names.Name.$").alias("country_name_raw"),
            "raw_json",
            "run_id",
            F.col("_source_file_modification_time").alias("source_file_modification_ts"),
            F.col("_source_file_path").alias("source_file_path"),
        )
        .withColumn("country_code", F.upper(F.trim(F.col("country_code_raw"))))
        .withColumn("country_name", F.trim(F.col("country_name_raw")))
    )

    return staged_df


@dp.materialized_view(
    name=QUARANTINE_TABLE,
    comment="Invalid country rows from the latest countries snapshot."
)
def silver_countries_quarantine():
    """
    Expose invalid country rows from the latest snapshot for investigation.
    """
    staged_df = spark.read.table("countries_staged_tmp")

    invalid_df = staged_df.filter(IS_INVALID_COUNTRY_ROW)

    quarantine_df = (
        invalid_df
        .withColumn(
            "validation_error",
            F.when(
                IS_INVALID_COUNTRY_CODE,
                F.lit("country_code missing or invalid"),
            ).otherwise(
                F.lit("country_name missing or invalid")
            ),
        )
        .withColumn("quarantined_ts", F.current_timestamp())
        .select(
            "country_code_raw",
            "country_name_raw",
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
    comment="Latest valid countries snapshot prepared for snapshot-based AUTO CDC."
)
@dp.expect_all_or_drop({
    "valid_country_code": VALID_COUNTRY_CODE_EXPECTATION,
    "valid_country_name": VALID_COUNTRY_NAME_EXPECTATION,
})
def silver_countries_snapshot_valid():
    """
    Prepare the latest valid countries snapshot for downstream processing.

    - Filters out invalid records using expectations
    - Selects relevant business and metadata columns
    - Deduplicates by country_code to ensure one record per key

    This dataset serves as the clean input snapshot for AUTO CDC (SCD Type 1).
    """
    staged_df = spark.read.table("countries_staged_tmp")

    valid_df = staged_df.select(
        "country_code",
        "country_name",
        "source_file_path",
        "source_file_modification_ts",
        "run_id",
    ).dropDuplicates(["country_code"])

    return valid_df


dp.create_streaming_table(
    name=CURRENT_TABLE,
    comment="SCD Type 1 current countries table for Lufthansa reference data."
)

dp.create_auto_cdc_from_snapshot_flow(
    target=CURRENT_TABLE,
    source=VALID_SNAPSHOT_TABLE,
    keys=["country_code"],
    stored_as_scd_type=1,
)




