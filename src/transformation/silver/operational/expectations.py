from pyspark.sql import functions as F


# Missing field checks
IS_MISSING_OPERATING_AIRLINE_ID = F.col("operating_airline_id").isNull()
IS_MISSING_OPERATING_FLIGHT_NUMBER = F.col("operating_flight_number").isNull()
IS_MISSING_DEPARTURE_AIRPORT_CODE = F.col("departure_airport_code").isNull()
IS_MISSING_ARRIVAL_AIRPORT_CODE = F.col("arrival_airport_code").isNull()
IS_MISSING_SCHEDULED_DEPARTURE_UTC_TS = F.col("scheduled_departure_utc_ts").isNull()
IS_MISSING_FLIGHT_DATE = F.col("flight_date").isNull()


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