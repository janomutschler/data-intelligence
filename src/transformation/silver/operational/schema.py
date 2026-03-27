from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

flight_struct = StructType([
    StructField("Departure", StructType([
        StructField("AirportCode", StringType(), True),
        StructField("ScheduledTimeLocal", StructType([
            StructField("DateTime", StringType(), True),
        ]), True),
        StructField("ScheduledTimeUTC", StructType([
            StructField("DateTime", StringType(), True),
        ]), True),
        StructField("ActualTimeLocal", StructType([
            StructField("DateTime", StringType(), True),
        ]), True),
        StructField("ActualTimeUTC", StructType([
            StructField("DateTime", StringType(), True),
        ]), True),
        StructField("TimeStatus", StructType([
            StructField("Code", StringType(), True),
            StructField("Definition", StringType(), True),
        ]), True),
        StructField("Terminal", StructType([
            StructField("Name", StringType(), True),
            StructField("Gate", StringType(), True),
        ]), True),
    ]), True),

    StructField("Arrival", StructType([
        StructField("AirportCode", StringType(), True),
        StructField("ScheduledTimeLocal", StructType([
            StructField("DateTime", StringType(), True),
        ]), True),
        StructField("ScheduledTimeUTC", StructType([
            StructField("DateTime", StringType(), True),
        ]), True),
        StructField("ActualTimeLocal", StructType([
            StructField("DateTime", StringType(), True),
        ]), True),
        StructField("ActualTimeUTC", StructType([
            StructField("DateTime", StringType(), True),
        ]), True),
        StructField("TimeStatus", StructType([
            StructField("Code", StringType(), True),
            StructField("Definition", StringType(), True),
        ]), True),
    ]), True),

    StructField("MarketingCarrier", StructType([
        StructField("AirlineID", StringType(), True),
        StructField("FlightNumber", StringType(), True),
    ]), True),

    StructField("OperatingCarrier", StructType([
        StructField("AirlineID", StringType(), True),
        StructField("FlightNumber", StringType(), True),
    ]), True),

    StructField("Equipment", StructType([
        StructField("AircraftCode", StringType(), True),
        StructField("AircraftRegistration", StringType(), True),
    ]), True),

    StructField("FlightStatus", StructType([
        StructField("Code", StringType(), True),
        StructField("Definition", StringType(), True),
    ]), True),

    StructField("ServiceType", StringType(), True),
])

flight_status_array_schema = StructType([
    StructField("FlightStatusResource", StructType([
        StructField("Flights", StructType([
            StructField("Flight", ArrayType(flight_struct), True),
        ]), True),
    ]), True),
])

flight_status_object_schema = StructType([
    StructField("FlightStatusResource", StructType([
        StructField("Flights", StructType([
            StructField("Flight", flight_struct, True),
        ]), True),
    ]), True),
])
