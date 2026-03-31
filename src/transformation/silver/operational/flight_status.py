from pyspark import pipelines as dp
from pyspark.sql import functions as F
from config import BRONZE_SCHEMA, SILVER_SCHEMA
from silver.operational.schema import (
    flight_status_array_schema,
    flight_status_object_schema,
)
from silver.operational.columns import (
    BASE_METADATA_COLUMNS,
    BASE_FLIGHT_COLUMNS,
    STAGED_FLIGHT_COLUMNS,
    CDC_SOURCE_COLUMNS,
)
from silver.operational.expressions import (
    build_flight_instance_hash_key,
    build_quarantine_reason,
    build_delay_minutes,
    parse_local_timestamp,
    parse_utc_timestamp,
)
from silver.operational.expectations import (
    IS_INVALID_FLIGHT_ROW,
    FLIGHT_STATUS_EXPECTATIONS,
)

CATALOG = spark.conf.get("catalog")


BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.flight_status_raw"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.flight_status_quarantine"
CDC_SOURCE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.flight_status_cdc_source"
SCD2_TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.flight_status_history"
CURRENT_TARGET_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.flight_status_current"

@dp.temporary_view(name="flight_status_exploded_tmp")
def flight_status_exploded_tmp():
    """
    Read bronze flight status data incrementally, normalize array vs single-object payloads,
    and explode to one row per flight.
    """
    bronze_df = spark.readStream.table(BRONZE_TABLE)

    parsed_array_df = bronze_df.withColumn(
        "parsed_array_json",
        F.from_json(F.col("raw_json"), flight_status_array_schema),
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_object_json",
        F.from_json(F.col("raw_json"), flight_status_object_schema),
    )

    normalized_df = parsed_both_df.withColumn(
        "flights_array",
        F.when(
            F.col("parsed_array_json.FlightStatusResource.Flights.Flight").isNotNull(),
            F.col("parsed_array_json.FlightStatusResource.Flights.Flight"),
        ).otherwise(
            F.array(F.col("parsed_object_json.FlightStatusResource.Flights.Flight"))
        ),
    )

    exploded_df = normalized_df.withColumn(
        "flight",
        F.explode_outer(F.col("flights_array")),
    )

    return exploded_df


@dp.temporary_view(name="flight_status_base_tmp")
def flight_status_base_tmp():
    """
    Extract raw business fields from exploded flight rows.
    """
    exploded_df = spark.readStream.table("flight_status_exploded_tmp")

    return exploded_df.select(
        *BASE_METADATA_COLUMNS,
        *BASE_FLIGHT_COLUMNS,
    )


@dp.temporary_view(name="flight_status_times_tmp")
def flight_status_times_tmp():
    """
    Parse all operational date/time strings into timestamps.
    """
    base_df = spark.readStream.table("flight_status_base_tmp")

    times_df =  (
        base_df
        .withColumn("scheduled_departure_local_ts", parse_local_timestamp("scheduled_departure_local_raw"))
        .withColumn("scheduled_departure_utc_ts", parse_utc_timestamp("scheduled_departure_utc_raw"))
        .withColumn("actual_departure_local_ts", parse_local_timestamp("actual_departure_local_raw"))
        .withColumn("actual_departure_utc_ts", parse_utc_timestamp("actual_departure_utc_raw"))
        .withColumn("scheduled_arrival_local_ts", parse_local_timestamp("scheduled_arrival_local_raw"))
        .withColumn("scheduled_arrival_utc_ts", parse_utc_timestamp("scheduled_arrival_utc_raw"))
        .withColumn("actual_arrival_local_ts", parse_local_timestamp("actual_arrival_local_raw"))
        .withColumn("actual_arrival_utc_ts", parse_utc_timestamp("actual_arrival_utc_raw"))
    )

    return times_df


@dp.temporary_view(name="flight_status_enriched_tmp")
def flight_status_enriched_tmp():
    """
    Derive business keys, metrics, flags, request context, and lineage columns.
    """
    times_df = spark.readStream.table("flight_status_times_tmp")

    metrics_df = (
        times_df
        .withColumn(
            "flight_date",
            F.coalesce(
                F.to_date("scheduled_departure_local_ts"),
                F.to_date("scheduled_departure_utc_ts"),
            ),
        )
        .withColumn(
            "departure_delay_minutes",
            build_delay_minutes(
                "actual_departure_utc_ts",
                "scheduled_departure_utc_ts",
            ),
        )
        .withColumn(
            "arrival_delay_minutes",
            build_delay_minutes(
                "actual_arrival_utc_ts",
                "scheduled_arrival_utc_ts",
            ),
        )
    )

    flags_df = (
        metrics_df
        .withColumn("has_missing_status", F.col("flight_status_code").isNull())
        .withColumn(
            "is_landed",
            F.coalesce(F.col("flight_status_code") == F.lit("LD"), F.lit(False)),
        )
        .withColumn(
            "is_cancelled",
            F.coalesce(F.col("flight_status_code") == F.lit("CD"), F.lit(False)),
        )
        .withColumn(
            "is_diverted",
            F.coalesce(F.col("flight_status_code") == F.lit("DV"), F.lit(False)),
        )
        .withColumn(
            "is_rerouted",
            F.coalesce(F.col("flight_status_code") == F.lit("RT"), F.lit(False)),
        )
    )

    request_context_df = (
        flags_df
        .withColumn("requested_direction", F.col("direction"))
        .withColumn("requested_airport", F.col("airport"))
        .withColumn("requested_date", F.col("date"))
        .withColumn("requested_window_start", F.col("window_start"))
        .withColumn("sequence_ts", F.col("_source_file_modification_time"))
    )
    

    enriched_df = (
        request_context_df
        .withColumn("flight_instance_hash_key", build_flight_instance_hash_key())
    )

    return enriched_df


@dp.temporary_view(name="flight_status_staged_tmp")
def flight_status_staged_tmp():
    """
    Final staged operational flight status dataset before validation and CDC preparation.
    """
    enriched_df = spark.readStream.table("flight_status_enriched_tmp")

    return enriched_df.select(
        *STAGED_FLIGHT_COLUMNS,
        *BASE_METADATA_COLUMNS,
    )


@dp.table(
    name=QUARANTINE_TABLE,
    comment="Invalid operational flight status rows for investigation."
)
def silver_flight_status_quarantine():
    """
    Expose invalid operational rows with quarantine reasons.
    """
    staged_df = spark.readStream.table("flight_status_staged_tmp")

    quarantine_df = (
        staged_df
        .filter(IS_INVALID_FLIGHT_ROW)
        .withColumn("quarantine_reason", build_quarantine_reason())
        .withColumn("quarantined_at", F.current_timestamp())
        .select(
            *BASE_METADATA_COLUMNS,
            "quarantine_reason",
            "quarantined_at",
        )
    )

    return quarantine_df


@dp.table(
    name=CDC_SOURCE_TABLE,
    private=True,
    comment="Validated operational flight status source prepared for downstream CDC processing."
)
@dp.expect_all_or_drop(FLIGHT_STATUS_EXPECTATIONS)
def silver_flight_status_cdc_source():
    """
    Produce the clean operational flight status source that will later feed AUTO CDC.
    """
    staged_df = spark.readStream.table("flight_status_staged_tmp")

    valid_df = staged_df.select(*CDC_SOURCE_COLUMNS)

    return valid_df


dp.create_streaming_table(
    name=SCD2_TARGET_TABLE,
    comment="SCD Type 2 history table for operational flight status data."
)


dp.create_auto_cdc_flow(
    target=SCD2_TARGET_TABLE,
    source=CDC_SOURCE_TABLE,
    keys=["flight_instance_hash_key"],
    sequence_by=F.col("sequence_ts"),
    stored_as_scd_type=2,
)

@dp.materialized_view(
    name=CURRENT_TARGET_TABLE,
    comment="Current active version of operational flight status records."
)
def silver_flight_status_current():
    history_df = spark.read.table(SCD2_TARGET_TABLE)

    return history_df.filter(F.col("__END_AT").isNull())