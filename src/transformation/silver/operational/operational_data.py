from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from silver.common.pipeline_state import (
    ensure_state_table,
    get_latest_processed_run_id,
    update_latest_processed_run_id,
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



def ensure_quarantine_table(spark, quarantine_table: str):
    if not spark.catalog.tableExists(quarantine_table):
        schema = """
            raw_json string,
            direction string,
            airport string,
            date date,
            window_start string,
            run_id string,
            _source_file_path string,
            _source_file_name string,
            _source_file_size long,
            _source_file_modification_time timestamp,
            _ingested_at timestamp,
            quarantine_reason string,
            quarantined_at timestamp
        """
        df = spark.createDataFrame([], schema)
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(quarantine_table)
        )


def ensure_silver_table(spark, silver_table: str):
    if not spark.catalog.tableExists(silver_table):
        schema = """
            flight_instance_key string,
            flight_date date,

            operating_airline_id string,
            operating_flight_number string,
            marketing_airline_id string,
            marketing_flight_number string,

            departure_airport_code string,
            arrival_airport_code string,

            scheduled_departure_local_ts timestamp,
            scheduled_departure_utc_ts timestamp,
            actual_departure_local_ts timestamp,
            actual_departure_utc_ts timestamp,

            scheduled_arrival_local_ts timestamp,
            scheduled_arrival_utc_ts timestamp,
            actual_arrival_local_ts timestamp,
            actual_arrival_utc_ts timestamp,

            initial_scheduled_departure_local_ts timestamp,
            initial_scheduled_departure_utc_ts timestamp,
            initial_scheduled_arrival_local_ts timestamp,
            initial_scheduled_arrival_utc_ts timestamp,

            departure_time_status_code string,
            departure_time_status_definition string,
            departure_terminal_name string,
            departure_gate string,

            arrival_time_status_code string,
            arrival_time_status_definition string,

            aircraft_code string,
            aircraft_registration string,

            flight_status_code string,
            flight_status_definition string,
            service_type string,

            departure_delay_minutes int,
            arrival_delay_minutes int,
            has_missing_status boolean,
            is_landed boolean,
            is_cancelled boolean,

            requested_direction string,
            requested_airport string,
            requested_date date,
            requested_window_start string,

            run_id string,
            _source_file_path string,
            _source_file_name string,
            _source_file_size long,
            _source_file_modification_time timestamp,
            _ingested_at timestamp,
            silver_processed_at timestamp,

            record_hash string
        """
        df = spark.createDataFrame([], schema)
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table)
        )



def normalize_nested_records(
    df,
    raw_json_col: str,
    array_schema: StructType,
    object_schema: StructType,
    array_path: str,
    object_path: str,
    output_array_col: str,
):
    """
    Normalize a nested JSON field that may arrive either as:
    - an array of objects
    - a single object

    Returns the input dataframe plus a normalized array column.
    """

    parsed_array_df = df.withColumn(
        "parsed_array_json",
        F.from_json(F.col(raw_json_col), array_schema)
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_object_json",
        F.from_json(F.col(raw_json_col), object_schema)
    )

    normalized_df = parsed_both_df.withColumn(
        "flights_array",
        F.when(
            F.col("parsed_array_json.FlightStatusResource.Flights.Flight").isNotNull(),
            F.col("parsed_array_json.FlightStatusResource.Flights.Flight")
        ).otherwise(
            F.array(
                F.col("parsed_object_json.FlightStatusResource.Flights.Flight")
            )
        )
    )

    return normalized_df



def process_operational_run(
    spark,
    bronze_table: str,
    silver_table: str,
    quarantine_table: str,
    run_id: str,
):
    bronze_df = spark.table(bronze_table).filter(F.col("run_id") == run_id)

    if bronze_df.limit(1).count() == 0:
        print(f"No bronze rows found for run_id={run_id}.")
        return

    normalized_df = normalize_nested_records(
        df=bronze_df,
        raw_json_col="raw_json",
        array_schema=flight_status_array_schema,
        object_schema=flight_status_object_schema,
        array_path="FlightStatusResource.Flights.Flight",
        object_path="FlightStatusResource.Flights.Flight",
        output_array_col="flights_array",
    )

    exploded_df = normalized_df.withColumn(
        "flight",
        F.explode_outer(F.col("flights_array"))
    )

    flattened_df = (
        exploded_df
        .select(
            "raw_json",
            "direction",
            "airport",
            "date",
            "window_start",
            "run_id",
            "_source_file_path",
            "_source_file_name",
            "_source_file_size",
            "_source_file_modification_time",
            "_ingested_at",

            F.col("flight.OperatingCarrier.AirlineID").alias("operating_airline_id"),
            F.col("flight.OperatingCarrier.FlightNumber").alias("operating_flight_number"),
            F.col("flight.MarketingCarrier.AirlineID").alias("marketing_airline_id"),
            F.col("flight.MarketingCarrier.FlightNumber").alias("marketing_flight_number"),

            F.col("flight.Departure.AirportCode").alias("departure_airport_code"),
            F.col("flight.Arrival.AirportCode").alias("arrival_airport_code"),

            F.to_timestamp(
                F.col("flight.Departure.ScheduledTimeLocal.DateTime"),
                "yyyy-MM-dd'T'HH:mm"
            ).alias("scheduled_departure_local_ts"),
            F.to_timestamp(
                F.col("flight.Departure.ScheduledTimeUTC.DateTime"),
                "yyyy-MM-dd'T'HH:mmX"
            ).alias("scheduled_departure_utc_ts"),
            F.to_timestamp(
                F.col("flight.Departure.ActualTimeLocal.DateTime"),
                "yyyy-MM-dd'T'HH:mm"
            ).alias("actual_departure_local_ts"),
            F.to_timestamp(
                F.col("flight.Departure.ActualTimeUTC.DateTime"),
                "yyyy-MM-dd'T'HH:mmX"
            ).alias("actual_departure_utc_ts"),

            F.to_timestamp(
                F.col("flight.Arrival.ScheduledTimeLocal.DateTime"),
                "yyyy-MM-dd'T'HH:mm"
            ).alias("scheduled_arrival_local_ts"),
            F.to_timestamp(
                F.col("flight.Arrival.ScheduledTimeUTC.DateTime"),
                "yyyy-MM-dd'T'HH:mmX"
            ).alias("scheduled_arrival_utc_ts"),
            F.to_timestamp(
                F.col("flight.Arrival.ActualTimeLocal.DateTime"),
                "yyyy-MM-dd'T'HH:mm"
            ).alias("actual_arrival_local_ts"),
            F.to_timestamp(
                F.col("flight.Arrival.ActualTimeUTC.DateTime"),
                "yyyy-MM-dd'T'HH:mmX"
            ).alias("actual_arrival_utc_ts"),

            F.col("flight.Departure.TimeStatus.Code").alias("departure_time_status_code"),
            F.col("flight.Departure.TimeStatus.Definition").alias("departure_time_status_definition"),
            F.col("flight.Departure.Terminal.Name").alias("departure_terminal_name"),
            F.col("flight.Departure.Terminal.Gate").alias("departure_gate"),

            F.col("flight.Arrival.TimeStatus.Code").alias("arrival_time_status_code"),
            F.col("flight.Arrival.TimeStatus.Definition").alias("arrival_time_status_definition"),

            F.col("flight.Equipment.AircraftCode").alias("aircraft_code"),
            F.col("flight.Equipment.AircraftRegistration").alias("aircraft_registration"),

            F.col("flight.FlightStatus.Code").alias("flight_status_code"),
            F.col("flight.FlightStatus.Definition").alias("flight_status_definition"),
            F.col("flight.ServiceType").alias("service_type"),
        )
        .withColumn(
            "flight_date",
            F.coalesce(
                F.to_date("scheduled_departure_local_ts"),
                F.to_date("scheduled_departure_utc_ts")
            )
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
                256
            )
        )
        .withColumn(
            "departure_delay_minutes",
            (
                (
                    F.col("actual_departure_utc_ts").cast("long")
                    - F.col("scheduled_departure_utc_ts").cast("long")
                ) / 60
            ).cast("int")
        )
        .withColumn(
            "arrival_delay_minutes",
            (
                (
                    F.col("actual_arrival_utc_ts").cast("long")
                    - F.col("scheduled_arrival_utc_ts").cast("long")
                ) / 60
            ).cast("int")
        )
        .withColumn("has_missing_status", F.col("flight_status_code").isNull())
        .withColumn(
            "is_landed",
            F.coalesce(F.col("flight_status_code") == F.lit("LD"), F.lit(False))
        )
        .withColumn(
            "is_cancelled",
            F.coalesce(F.col("flight_status_code") == F.lit("CD"), F.lit(False))
        )
        .withColumn("requested_direction", F.col("direction"))
        .withColumn("requested_airport", F.col("airport"))
        .withColumn("requested_date", F.col("date"))
        .withColumn("requested_window_start", F.col("window_start"))
        .withColumn("initial_scheduled_departure_local_ts", F.col("scheduled_departure_local_ts"))
        .withColumn("initial_scheduled_departure_utc_ts", F.col("scheduled_departure_utc_ts"))
        .withColumn("initial_scheduled_arrival_local_ts", F.col("scheduled_arrival_local_ts"))
        .withColumn("initial_scheduled_arrival_utc_ts", F.col("scheduled_arrival_utc_ts"))
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    validated_df = (
        flattened_df
        .withColumn("missing_operating_airline_id", F.col("operating_airline_id").isNull())
        .withColumn("missing_operating_flight_number", F.col("operating_flight_number").isNull())
        .withColumn("missing_departure_airport_code", F.col("departure_airport_code").isNull())
        .withColumn("missing_arrival_airport_code", F.col("arrival_airport_code").isNull())
        .withColumn("missing_scheduled_departure_utc_ts", F.col("scheduled_departure_utc_ts").isNull())
        .withColumn("missing_flight_date", F.col("flight_date").isNull())
        .withColumn(
            "is_invalid",
            F.col("missing_operating_airline_id") |
            F.col("missing_operating_flight_number") |
            F.col("missing_departure_airport_code") |
            F.col("missing_arrival_airport_code") |
            F.col("missing_scheduled_departure_utc_ts") |
            F.col("missing_flight_date")
        )
    )

    quarantine_df = (
        validated_df
        .filter(F.col("is_invalid"))
        .withColumn(
            "quarantine_reason",
            F.concat_ws(
                ", ",
                F.when(F.col("missing_operating_airline_id"), F.lit("missing_operating_airline_id")),
                F.when(F.col("missing_operating_flight_number"), F.lit("missing_operating_flight_number")),
                F.when(F.col("missing_departure_airport_code"), F.lit("missing_departure_airport_code")),
                F.when(F.col("missing_arrival_airport_code"), F.lit("missing_arrival_airport_code")),
                F.when(F.col("missing_scheduled_departure_utc_ts"), F.lit("missing_scheduled_departure_utc_ts")),
                F.when(F.col("missing_flight_date"), F.lit("missing_flight_date")),
            )
        )
        .select(
            "raw_json",
            "direction",
            "airport",
            "date",
            "window_start",
            "run_id",
            "_source_file_path",
            "_source_file_name",
            "_source_file_size",
            "_source_file_modification_time",
            "_ingested_at",
            "quarantine_reason",
            F.current_timestamp().alias("quarantined_at"),
        )
    )

    valid_df = (
        validated_df
        .filter(~F.col("is_invalid"))
        .drop(
            "missing_operating_airline_id",
            "missing_operating_flight_number",
            "missing_departure_airport_code",
            "missing_arrival_airport_code",
            "missing_scheduled_departure_utc_ts",
            "missing_flight_date",
            "is_invalid",
        )
    )

    if quarantine_df.limit(1).count() > 0:
        (
            quarantine_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(quarantine_table)
        )

    incoming_df = valid_df.withColumn(
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
                F.coalesce(F.col("flight_status_code"), F.lit("")),
                F.coalesce(F.col("flight_status_definition"), F.lit("")),
                F.coalesce(F.col("aircraft_code"), F.lit("")),
                F.coalesce(F.col("aircraft_registration"), F.lit("")),
                F.coalesce(F.col("departure_terminal_name"), F.lit("")),
                F.coalesce(F.col("departure_gate"), F.lit("")),
            ),
            256
        )
    )

    deduped_df = (
        incoming_df
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("flight_instance_key").orderBy(
                    F.col("_ingested_at").desc_nulls_last(),
                    F.col("_source_file_modification_time").desc_nulls_last(),
                )
            )
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    silver_columns = spark.table(silver_table).columns
    merge_source_df = deduped_df.select(*silver_columns)
    silver_dt = DeltaTable.forName(spark, silver_table)

    update_set = {
        "flight_date": "s.flight_date",
        "operating_airline_id": "s.operating_airline_id",
        "operating_flight_number": "s.operating_flight_number",
        "marketing_airline_id": "s.marketing_airline_id",
        "marketing_flight_number": "s.marketing_flight_number",
        "departure_airport_code": "s.departure_airport_code",
        "arrival_airport_code": "s.arrival_airport_code",
        "scheduled_departure_local_ts": "s.scheduled_departure_local_ts",
        "scheduled_departure_utc_ts": "s.scheduled_departure_utc_ts",
        "actual_departure_local_ts": "s.actual_departure_local_ts",
        "actual_departure_utc_ts": "s.actual_departure_utc_ts",
        "scheduled_arrival_local_ts": "s.scheduled_arrival_local_ts",
        "scheduled_arrival_utc_ts": "s.scheduled_arrival_utc_ts",
        "actual_arrival_local_ts": "s.actual_arrival_local_ts",
        "actual_arrival_utc_ts": "s.actual_arrival_utc_ts",
        "initial_scheduled_departure_local_ts": "coalesce(t.initial_scheduled_departure_local_ts, s.initial_scheduled_departure_local_ts)",
        "initial_scheduled_departure_utc_ts": "coalesce(t.initial_scheduled_departure_utc_ts, s.initial_scheduled_departure_utc_ts)",
        "initial_scheduled_arrival_local_ts": "coalesce(t.initial_scheduled_arrival_local_ts, s.initial_scheduled_arrival_local_ts)",
        "initial_scheduled_arrival_utc_ts": "coalesce(t.initial_scheduled_arrival_utc_ts, s.initial_scheduled_arrival_utc_ts)",
        "departure_time_status_code": "s.departure_time_status_code",
        "departure_time_status_definition": "s.departure_time_status_definition",
        "departure_terminal_name": "s.departure_terminal_name",
        "departure_gate": "s.departure_gate",
        "arrival_time_status_code": "s.arrival_time_status_code",
        "arrival_time_status_definition": "s.arrival_time_status_definition",
        "aircraft_code": "s.aircraft_code",
        "aircraft_registration": "s.aircraft_registration",
        "flight_status_code": "s.flight_status_code",
        "flight_status_definition": "s.flight_status_definition",
        "service_type": "s.service_type",
        "departure_delay_minutes": "s.departure_delay_minutes",
        "arrival_delay_minutes": "s.arrival_delay_minutes",
        "has_missing_status": "s.has_missing_status",
        "is_landed": "s.is_landed",
        "is_cancelled": "s.is_cancelled",
        "requested_direction": "s.requested_direction",
        "requested_airport": "s.requested_airport",
        "requested_date": "s.requested_date",
        "requested_window_start": "s.requested_window_start",
        "run_id": "s.run_id",
        "_source_file_path": "s._source_file_path",
        "_source_file_name": "s._source_file_name",
        "_source_file_size": "s._source_file_size",
        "_source_file_modification_time": "s._source_file_modification_time",
        "_ingested_at": "s._ingested_at",
        "silver_processed_at": "s.silver_processed_at",
        "record_hash": "s.record_hash",
    }

    insert_set = {col: f"s.{col}" for col in merge_source_df.columns}

    (
        silver_dt.alias("t")
        .merge(
            merge_source_df.alias("s"),
            "t.flight_instance_key = s.flight_instance_key"
        )
        .whenMatchedUpdate(
            condition="t.record_hash <> s.record_hash",
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )

    print(f"Finished processing operatilonal run_id={run_id}")

def process_operational_data(
    spark,
    bronze_table: str,
    silver_table: str,
    quarantine_table: str,
    state_table: str,
    entity_name: str,
):


    ensure_silver_table(spark, silver_table)
    ensure_quarantine_table(spark, quarantine_table)

    latest_run_id = get_latest_processed_run_id(
        spark,
        state_table,
        entity_name,
    )
    
    runs_df = spark.table(bronze_table).select("run_id").distinct()

    if latest_run_id is not None:
        runs_df = runs_df.filter(F.col("run_id") > F.lit(latest_run_id))

    run_ids = [
        row["run_id"]
        for row in runs_df.orderBy("run_id").collect()
        if row["run_id"] is not None
    ]

    if not run_ids:
        print(f"No new runs to process for {entity_name}.")
        return

    for run_id in run_ids:
        process_operational_run(
            spark=spark,
            bronze_table=bronze_table,
            silver_table=silver_table,
            quarantine_table=quarantine_table,
            run_id=run_id,
        )

        update_latest_processed_run_id(
            spark=spark,
            state_table=state_table,
            entity_name=entity_name,
            latest_run_id=run_id,
        )