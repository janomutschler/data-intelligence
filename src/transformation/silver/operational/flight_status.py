from pyspark import pipelines as dp
from pyspark.sql import functions as F
from flight_status_schema import (
    flight_status_array_schema,
    flight_status_object_schema,
)
from columns import (
    BASE_METADATA_COLUMNS,
    BASE_FLIGHT_COLUMNS,
    STAGED_FLIGHT_COLUMNS,
    CDC_SOURCE_COLUMNS,
)

CATALOG = spark.conf.get("catalog")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_flight_status_raw"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_flight_status_quarantine"
CDC_SOURCE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.silver_flight_status_cdc_source"


IS_MISSING_OPERATING_AIRLINE_ID = F.col("operating_airline_id").isNull()
IS_MISSING_OPERATING_FLIGHT_NUMBER = F.col("operating_flight_number").isNull()
IS_MISSING_DEPARTURE_AIRPORT_CODE = F.col("departure_airport_code").isNull()
IS_MISSING_ARRIVAL_AIRPORT_CODE = F.col("arrival_airport_code").isNull()
IS_MISSING_SCHEDULED_DEPARTURE_UTC_TS = F.col("scheduled_departure_utc_ts").isNull()
IS_MISSING_FLIGHT_DATE = F.col("flight_date").isNull()

IS_INVALID_FLIGHT_ROW = (
    IS_MISSING_OPERATING_AIRLINE_ID |
    IS_MISSING_OPERATING_FLIGHT_NUMBER |
    IS_MISSING_DEPARTURE_AIRPORT_CODE |
    IS_MISSING_ARRIVAL_AIRPORT_CODE |
    IS_MISSING_SCHEDULED_DEPARTURE_UTC_TS |
    IS_MISSING_FLIGHT_DATE
)

VALID_OPERATING_AIRLINE_ID_EXPECTATION = "operating_airline_id IS NOT NULL"
VALID_OPERATING_FLIGHT_NUMBER_EXPECTATION = "operating_flight_number IS NOT NULL"
VALID_DEPARTURE_AIRPORT_CODE_EXPECTATION = "departure_airport_code IS NOT NULL"
VALID_ARRIVAL_AIRPORT_CODE_EXPECTATION = "arrival_airport_code IS NOT NULL"
VALID_SCHEDULED_DEPARTURE_UTC_TS_EXPECTATION = "scheduled_departure_utc_ts IS NOT NULL"
VALID_FLIGHT_DATE_EXPECTATION = "flight_date IS NOT NULL"


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

    return (
        base_df
        .withColumn(
            "scheduled_departure_local_ts",
            F.to_timestamp("scheduled_departure_local_raw", "yyyy-MM-dd'T'HH:mm"),
        )
        .withColumn(
            "scheduled_departure_utc_ts",
            F.to_timestamp("scheduled_departure_utc_raw", "yyyy-MM-dd'T'HH:mmX"),
        )
        .withColumn(
            "actual_departure_local_ts",
            F.to_timestamp("actual_departure_local_raw", "yyyy-MM-dd'T'HH:mm"),
        )
        .withColumn(
            "actual_departure_utc_ts",
            F.to_timestamp("actual_departure_utc_raw", "yyyy-MM-dd'T'HH:mmX"),
        )
        .withColumn(
            "scheduled_arrival_local_ts",
            F.to_timestamp("scheduled_arrival_local_raw", "yyyy-MM-dd'T'HH:mm"),
        )
        .withColumn(
            "scheduled_arrival_utc_ts",
            F.to_timestamp("scheduled_arrival_utc_raw", "yyyy-MM-dd'T'HH:mmX"),
        )
        .withColumn(
            "actual_arrival_local_ts",
            F.to_timestamp("actual_arrival_local_raw", "yyyy-MM-dd'T'HH:mm"),
        )
        .withColumn(
            "actual_arrival_utc_ts",
            F.to_timestamp("actual_arrival_utc_raw", "yyyy-MM-dd'T'HH:mmX"),
        )
    )


@dp.temporary_view(name="flight_status_enriched_tmp")
def flight_status_enriched_tmp():
    """
    Derive business keys, metrics, flags, and lineage columns from parsed flight status data.
    """
    times_df = spark.readStream.table("flight_status_times_tmp")

    return (
        times_df
        .withColumn(
            "flight_date",
            F.coalesce(
                F.to_date("scheduled_departure_local_ts"),
                F.to_date("scheduled_departure_utc_ts"),
            ),
        )
        .withColumn(
            "flight_instance_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("operating_airline_id"), F.lit("")),
                    F.coalesce(F.col("operating_flight_number"), F.lit("")),
                    F.coalesce(F.col("departure_airport_code"), F.lit("")),
                    F.coalesce(F.col("arrival_airport_code"), F.lit("")),
                    F.coalesce(F.col("flight_date").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn(
            "departure_delay_minutes",
            (
                (
                    F.col("actual_departure_utc_ts").cast("long")
                    - F.col("scheduled_departure_utc_ts").cast("long")
                ) / 60
            ).cast("int"),
        )
        .withColumn(
            "arrival_delay_minutes",
            (
                (
                    F.col("actual_arrival_utc_ts").cast("long")
                    - F.col("scheduled_arrival_utc_ts").cast("long")
                ) / 60
            ).cast("int"),
        )
        .withColumn("has_missing_status", F.col("flight_status_code").isNull())
        .withColumn("is_landed", F.coalesce(F.col("flight_status_code") == F.lit("LD"), F.lit(False)))
        .withColumn("is_cancelled", F.coalesce(F.col("flight_status_code") == F.lit("CD"), F.lit(False)))
        .withColumn("requested_direction", F.col("direction"))
        .withColumn("requested_airport", F.col("airport"))
        .withColumn("requested_date", F.col("date"))
        .withColumn("requested_window_start", F.col("window_start"))
        .withColumn("sequence_ts", F.col("_source_file_modification_time"))
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("operating_airline_id"), F.lit("")),
                    F.coalesce(F.col("operating_flight_number"), F.lit("")),
                    F.coalesce(F.col("marketing_airline_id"), F.lit("")),
                    F.coalesce(F.col("marketing_flight_number"), F.lit("")),
                    F.coalesce(F.col("departure_airport_code"), F.lit("")),
                    F.coalesce(F.col("arrival_airport_code"), F.lit("")),
                    F.coalesce(F.col("scheduled_departure_local_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("scheduled_departure_utc_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("actual_departure_local_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("actual_departure_utc_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("scheduled_arrival_local_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("scheduled_arrival_utc_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("actual_arrival_local_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("actual_arrival_utc_ts").cast("string"), F.lit("")),
                    F.coalesce(F.col("departure_time_status_code"), F.lit("")),
                    F.coalesce(F.col("departure_time_status_definition"), F.lit("")),
                    F.coalesce(F.col("departure_terminal_name"), F.lit("")),
                    F.coalesce(F.col("departure_gate"), F.lit("")),
                    F.coalesce(F.col("arrival_time_status_code"), F.lit("")),
                    F.coalesce(F.col("arrival_time_status_definition"), F.lit("")),
                    F.coalesce(F.col("aircraft_code"), F.lit("")),
                    F.coalesce(F.col("aircraft_registration"), F.lit("")),
                    F.coalesce(F.col("flight_status_code"), F.lit("")),
                    F.coalesce(F.col("flight_status_definition"), F.lit("")),
                    F.coalesce(F.col("service_type"), F.lit("")),
                ),
                256,
            ),
        )
    )

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
        .withColumn(
            "quarantine_reason",
            F.concat_ws(
                ", ",
                F.when(IS_MISSING_OPERATING_AIRLINE_ID, F.lit("missing_operating_airline_id")),
                F.when(IS_MISSING_OPERATING_FLIGHT_NUMBER, F.lit("missing_operating_flight_number")),
                F.when(IS_MISSING_DEPARTURE_AIRPORT_CODE, F.lit("missing_departure_airport_code")),
                F.when(IS_MISSING_ARRIVAL_AIRPORT_CODE, F.lit("missing_arrival_airport_code")),
                F.when(IS_MISSING_SCHEDULED_DEPARTURE_UTC_TS, F.lit("missing_scheduled_departure_utc_ts")),
                F.when(IS_MISSING_FLIGHT_DATE, F.lit("missing_flight_date")),
            ),
        )
        .withColumn("quarantined_at", F.current_timestamp())
        .select(
            "raw_json",
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
@dp.expect_all_or_drop({
    "valid_operating_airline_id": VALID_OPERATING_AIRLINE_ID_EXPECTATION,
    "valid_operating_flight_number": VALID_OPERATING_FLIGHT_NUMBER_EXPECTATION,
    "valid_departure_airport_code": VALID_DEPARTURE_AIRPORT_CODE_EXPECTATION,
    "valid_arrival_airport_code": VALID_ARRIVAL_AIRPORT_CODE_EXPECTATION,
    "valid_scheduled_departure_utc_ts": VALID_SCHEDULED_DEPARTURE_UTC_TS_EXPECTATION,
    "valid_flight_date": VALID_FLIGHT_DATE_EXPECTATION,
})
def silver_flight_status_cdc_source():
    """
    Produce the clean operational flight status source that will later feed AUTO CDC.
    """
    staged_df = spark.readStream.table("flight_status_staged_tmp")

    valid_df = staged_df.select(*CDC_SOURCE_COLUMNS)

    return valid_df