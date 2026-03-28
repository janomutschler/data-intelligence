from pyspark.sql import functions as F
from silver.operational.expectations import (
    IS_MISSING_OPERATING_AIRLINE_ID,
    IS_MISSING_OPERATING_FLIGHT_NUMBER,
    IS_MISSING_DEPARTURE_AIRPORT_CODE,
    IS_MISSING_ARRIVAL_AIRPORT_CODE,
    IS_MISSING_SCHEDULED_DEPARTURE_UTC_TS,
    IS_MISSING_FLIGHT_DATE,
)


def build_flight_instance_hash_key():
    return F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("operating_airline_id"), F.lit("")),
            F.coalesce(F.col("operating_flight_number"), F.lit("")),
            F.coalesce(F.col("departure_airport_code"), F.lit("")),
            F.coalesce(F.col("flight_date").cast("string"), F.lit("")),
        ),
        256,
    )


def build_delay_minutes(actual_col: str, scheduled_col: str):
    return (
        (
            F.col(actual_col).cast("long") - F.col(scheduled_col).cast("long")
        ) / 60
    ).cast("int")

 
def parse_local_timestamp(column_name: str):
    return F.to_timestamp(F.col(column_name), "yyyy-MM-dd'T'HH:mm")


def parse_utc_timestamp(column_name: str):
    return F.to_timestamp(F.col(column_name), "yyyy-MM-dd'T'HH:mmX")


def build_quarantine_reason():
    return F.concat_ws(
        ", ",
        F.when(IS_MISSING_OPERATING_AIRLINE_ID, F.lit("missing_operating_airline_id")),
        F.when(IS_MISSING_OPERATING_FLIGHT_NUMBER, F.lit("missing_operating_flight_number")),
        F.when(IS_MISSING_DEPARTURE_AIRPORT_CODE, F.lit("missing_departure_airport_code")),
        F.when(IS_MISSING_ARRIVAL_AIRPORT_CODE, F.lit("missing_arrival_airport_code")),
        F.when(IS_MISSING_SCHEDULED_DEPARTURE_UTC_TS, F.lit("missing_scheduled_departure_utc_ts")),
        F.when(IS_MISSING_FLIGHT_DATE, F.lit("missing_flight_date")),
    )