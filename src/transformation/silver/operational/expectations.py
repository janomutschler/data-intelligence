from pyspark.sql import functions as F

def is_missing(col_name: str):
    return (
        F.col(col_name).isNull() |
        (F.trim(F.col(col_name)) == "")
    )

# Missing field checks
IS_MISSING_OPERATING_AIRLINE_ID = is_missing("operating_airline_id")
IS_MISSING_OPERATING_FLIGHT_NUMBER = is_missing("operating_flight_number")
IS_MISSING_DEPARTURE_AIRPORT_CODE = is_missing("departure_airport_code")
IS_MISSING_ARRIVAL_AIRPORT_CODE = is_missing("arrival_airport_code")
IS_MISSING_SCHEDULED_DEPARTURE_UTC_TS = is_missing("scheduled_departure_utc_ts")
IS_MISSING_FLIGHT_DATE = is_missing("flight_date")


# Composite validation
IS_INVALID_FLIGHT_ROW = (
    IS_MISSING_OPERATING_AIRLINE_ID |
    IS_MISSING_OPERATING_FLIGHT_NUMBER |
    IS_MISSING_DEPARTURE_AIRPORT_CODE |
    IS_MISSING_ARRIVAL_AIRPORT_CODE |
    IS_MISSING_SCHEDULED_DEPARTURE_UTC_TS |
    IS_MISSING_FLIGHT_DATE
)


# --- DLT expectations ---
FLIGHT_STATUS_EXPECTATIONS = {
    "valid_operating_airline_id": "operating_airline_id IS NOT NULL AND TRIM(operating_airline_id) <> ''",
    "valid_operating_flight_number": "operating_flight_number IS NOT NULL AND TRIM(operating_flight_number) <> ''",
    "valid_departure_airport_code": "departure_airport_code IS NOT NULL AND TRIM(departure_airport_code) <> ''",
    "valid_arrival_airport_code": "arrival_airport_code IS NOT NULL AND TRIM(arrival_airport_code) <> ''",
    "valid_scheduled_departure_utc_ts": "scheduled_departure_utc_ts IS NOT NULL",
    "valid_flight_date": "flight_date IS NOT NULL",
}