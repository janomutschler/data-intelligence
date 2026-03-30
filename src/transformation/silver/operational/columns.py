from pyspark.sql import functions as F

BASE_METADATA_COLUMNS = [
    "raw_json",
    "direction",
    "airport",
    "date",
    "window_start",
    "run_id",
    "_source_file_path",
    "_source_file_modification_time",
]


BASE_FLIGHT_COLUMNS = [
    # Airlines
    F.col("flight.OperatingCarrier.AirlineID").alias("operating_airline_id"),
    F.col("flight.OperatingCarrier.FlightNumber").alias("operating_flight_number"),
    F.col("flight.MarketingCarrier.AirlineID").alias("marketing_airline_id"),
    F.col("flight.MarketingCarrier.FlightNumber").alias("marketing_flight_number"),

    # Airports
    F.col("flight.Departure.AirportCode").alias("departure_airport_code"),
    F.col("flight.Arrival.AirportCode").alias("arrival_airport_code"),

    # Departure times (raw)
    F.col("flight.Departure.ScheduledTimeLocal.DateTime").alias("scheduled_departure_local_raw"),
    F.col("flight.Departure.ScheduledTimeUTC.DateTime").alias("scheduled_departure_utc_raw"),
    F.col("flight.Departure.ActualTimeLocal.DateTime").alias("actual_departure_local_raw"),
    F.col("flight.Departure.ActualTimeUTC.DateTime").alias("actual_departure_utc_raw"),

    # Arrival times (raw)
    F.col("flight.Arrival.ScheduledTimeLocal.DateTime").alias("scheduled_arrival_local_raw"),
    F.col("flight.Arrival.ScheduledTimeUTC.DateTime").alias("scheduled_arrival_utc_raw"),
    F.col("flight.Arrival.ActualTimeLocal.DateTime").alias("actual_arrival_local_raw"),
    F.col("flight.Arrival.ActualTimeUTC.DateTime").alias("actual_arrival_utc_raw"),

    # Departure status
    F.col("flight.Departure.TimeStatus.Code").alias("departure_time_status_code"),
    F.col("flight.Departure.TimeStatus.Definition").alias("departure_time_status_definition"),
    F.col("flight.Departure.Terminal.Name").alias("departure_terminal_name"),
    F.col("flight.Departure.Terminal.Gate").alias("departure_gate"),

    # Arrival status
    F.col("flight.Arrival.TimeStatus.Code").alias("arrival_time_status_code"),
    F.col("flight.Arrival.TimeStatus.Definition").alias("arrival_time_status_definition"),

    # Aircraft
    F.col("flight.Equipment.AircraftCode").alias("aircraft_code"),
    F.col("flight.Equipment.AircraftRegistration").alias("aircraft_registration"),

    # Flight status
    F.col("flight.FlightStatus.Code").alias("flight_status_code"),
    F.col("flight.FlightStatus.Definition").alias("flight_status_definition"),
    F.col("flight.ServiceType").alias("service_type"),
]


BASE_STAGED_COLUMNS = [
    # Keys
    "flight_instance_hash_key",
    "flight_date",

    # Airlines
    "operating_airline_id",
    "operating_flight_number",
    "marketing_airline_id",
    "marketing_flight_number",

    # Airports
    "departure_airport_code",
    "arrival_airport_code",

    # Departure times
    "scheduled_departure_local_ts",
    "scheduled_departure_utc_ts",
    "actual_departure_local_ts",
    "actual_departure_utc_ts",

    # Arrival times
    "scheduled_arrival_local_ts",
    "scheduled_arrival_utc_ts",
    "actual_arrival_local_ts",
    "actual_arrival_utc_ts",

    # Departure status
    "departure_time_status_code",
    "departure_time_status_definition",
    "departure_terminal_name",
    "departure_gate",

    # Arrival status
    "arrival_time_status_code",
    "arrival_time_status_definition",

    # Aircraft
    "aircraft_code",
    "aircraft_registration",

    # Flight status
    "flight_status_code",
    "flight_status_definition",
    "service_type",

    # Metrics
    "departure_delay_minutes",
    "arrival_delay_minutes",

    # Flags
    "has_missing_status",
    "is_landed",
    "is_cancelled",
	"is_diverted",
	"is_rerouted",

    # Request metadata
    "requested_direction",
    "requested_airport",
    "requested_date",
    "requested_window_start",

    # CDC
    "sequence_ts",
]


STAGED_FLIGHT_COLUMNS = BASE_STAGED_COLUMNS


CDC_SOURCE_COLUMNS = BASE_STAGED_COLUMNS + [
    "run_id",
    "_source_file_path",
    "_source_file_modification_time",
]