from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    DoubleType,
)
from silver.common.pipeline_state import (
    get_latest_processed_run_id,
    update_latest_processed_run_id,
)

airports_array_schema = StructType([
    StructField("AirportResource", StructType([
        StructField("Airports", StructType([
            StructField("Airport", ArrayType(
                StructType([
                    StructField("AirportCode", StringType(), True),
                    StructField("Position", StructType([
                        StructField("Coordinate", StructType([
                            StructField("Latitude", DoubleType(), True),
                            StructField("Longitude", DoubleType(), True),
                        ]), True)
                    ]), True),
                    StructField("CityCode", StringType(), True),
                    StructField("CountryCode", StringType(), True),
                    StructField("LocationType", StringType(), True),
                    StructField("Names", StructType([
                        StructField("Name", StructType([
                            StructField("@LanguageCode", StringType(), True),
                            StructField("$", StringType(), True),
                        ]), True)
                    ]), True),
                    StructField("UtcOffset", StringType(), True),
                    StructField("TimeZoneId", StringType(), True),
                ])
            ), True)
        ]), True)
    ]), True)
])

airports_single_schema = StructType([
    StructField("AirportResource", StructType([
        StructField("Airports", StructType([
            StructField("Airport", StructType([
                StructField("AirportCode", StringType(), True),
                StructField("Position", StructType([
                    StructField("Coordinate", StructType([
                        StructField("Latitude", DoubleType(), True),
                        StructField("Longitude", DoubleType(), True),
                    ]), True)
                ]), True),
                StructField("CityCode", StringType(), True),
                StructField("CountryCode", StringType(), True),
                StructField("LocationType", StringType(), True),
                StructField("Names", StructType([
                    StructField("Name", StructType([
                        StructField("@LanguageCode", StringType(), True),
                        StructField("$", StringType(), True),
                    ]), True)
                ]), True),
                StructField("UtcOffset", StringType(), True),
                StructField("TimeZoneId", StringType(), True),
            ]), True)
        ]), True)
    ]), True)
])

def ensure_history_table(spark, history_table: str):
    if not spark.catalog.tableExists(history_table):
        df = spark.createDataFrame(
            [],
            """
            airport_code string,
            airport_name string,
            city_code string,
            country_code string,
            latitude double,
            longitude double,
            location_type string,
            utc_offset string,
            time_zone_id string,
            observed_from_ts timestamp,
            observed_to_ts timestamp,
            last_seen_ts timestamp,
            is_current boolean,
            source_ingestion_ts timestamp,
            source_file_name string,
            created_by_run_id string,
            closed_by_run_id string
            """
        )
        df.write.format("delta").mode("overwrite").saveAsTable(history_table)

def ensure_current_table(spark, current_table: str):
    if not spark.catalog.tableExists(current_table):
        df = spark.createDataFrame(
            [],
            """
            airport_code string,
            airport_name string,
            city_code string,
            country_code string,
            latitude double,
            longitude double,
            location_type string,
            utc_offset string,
            time_zone_id string,
            observed_from_ts timestamp,
            last_seen_ts timestamp,
            source_ingestion_ts timestamp,
            source_file_name string,
            created_by_run_id string
            """
        )
        df.write.format("delta").mode("overwrite").saveAsTable(current_table)

def ensure_quarantine_table(spark, quarantine_table: str):
    if not spark.catalog.tableExists(quarantine_table):
        df = spark.createDataFrame(
            [],
            """
            airport_code_raw string,
            airport_name_raw string,
            city_code_raw string,
            country_code_raw string,
            latitude_raw double,
            longitude_raw double,
            location_type_raw string,
            utc_offset_raw string,
            time_zone_id_raw string,
            validation_error string,
            raw_json string,
            source_ingestion_ts timestamp,
            source_file_name string,
            run_id string,
            quarantined_ts timestamp
            """
        )
        df.write.format("delta").mode("overwrite").saveAsTable(quarantine_table)

def process_airports_run(
    spark,
    bronze_table: str,
    history_table: str,
    quarantine_table: str,
    run_id: str,
):
    """
    Loads one run of the airports bronze table and processes it to silver history.
    """
    bronze_df = spark.table(bronze_table).filter(F.col("run_id") == run_id)

    parsed_array_df = bronze_df.withColumn(
        "parsed_array_json",
        F.from_json(F.col("raw_json"), airports_array_schema)
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_single_json",
        F.from_json(F.col("raw_json"), airports_single_schema)
    )

    normalized_df = parsed_both_df.withColumn(
        "airports_array",
        F.when(
            F.col("parsed_array_json.AirportResource.Airports.Airport").isNotNull(),
            F.col("parsed_array_json.AirportResource.Airports.Airport")
        ).otherwise(
            F.array(F.col("parsed_single_json.AirportResource.Airports.Airport"))
        )
    )

    exploded_df = normalized_df.withColumn(
        "airport",
        F.explode_outer(F.col("airports_array"))
    )

    staged_df = (
        exploded_df
        .select(
            F.col("airport.AirportCode").alias("airport_code_raw"),
            F.col("airport.Names.Name.$").alias("airport_name_raw"),
            F.col("airport.CityCode").alias("city_code_raw"),
            F.col("airport.CountryCode").alias("country_code_raw"),
            F.col("airport.Position.Coordinate.Latitude").alias("latitude_raw"),
            F.col("airport.Position.Coordinate.Longitude").alias("longitude_raw"),
            F.col("airport.LocationType").alias("location_type_raw"),
            F.col("airport.UtcOffset").alias("utc_offset_raw"),
            F.col("airport.TimeZoneId").alias("time_zone_id_raw"),
            F.col("raw_json"),
            F.col("run_id"),
            F.col("_source_file_name"),
            F.col("_source_file_modification_time"),
            F.col("_ingested_at"),
        )
        .withColumn("airport_code", F.upper(F.trim(F.col("airport_code_raw"))))
        .withColumn("airport_name", F.trim(F.col("airport_name_raw")))
        .withColumn("city_code", F.upper(F.trim(F.col("city_code_raw"))))
        .withColumn("country_code", F.upper(F.trim(F.col("country_code_raw"))))
        .withColumn("latitude", F.col("latitude_raw"))
        .withColumn("longitude", F.col("longitude_raw"))
        .withColumn("location_type", F.trim(F.col("location_type_raw")))
        .withColumn("utc_offset", F.trim(F.col("utc_offset_raw")))
        .withColumn("time_zone_id", F.trim(F.col("time_zone_id_raw")))
        .withColumn(
            "observed_ts",
            F.coalesce(F.col("_source_file_modification_time"), F.col("_ingested_at"))
        )
    )

    invalid_df = staged_df.filter(
        F.col("airport_code").isNull() | (F.col("airport_code") == "") |
        F.col("airport_name").isNull() | (F.col("airport_name") == "") |
        F.col("city_code").isNull() | (F.col("city_code") == "") |
        F.col("country_code").isNull() | (F.col("country_code") == "")
    )

    valid_df = (
        staged_df
        .filter(
            F.col("airport_code").isNotNull() & (F.col("airport_code") != "") &
            F.col("airport_name").isNotNull() & (F.col("airport_name") != "") &
            F.col("city_code").isNotNull() & (F.col("city_code") != "") &
            F.col("country_code").isNotNull() & (F.col("country_code") != "")
        )
        .dropDuplicates(["airport_code"])
    )

    quarantine_df = (
        invalid_df
        .withColumn(
            "validation_error",
            F.when(
                F.col("airport_code").isNull() | (F.col("airport_code") == ""),
                F.lit("airport_code missing or invalid")
            ).when(
                F.col("airport_name").isNull() | (F.col("airport_name") == ""),
                F.lit("airport_name missing or invalid")
            ).when(
                F.col("city_code").isNull() | (F.col("city_code") == ""),
                F.lit("city_code missing or invalid")
            ).otherwise(F.lit("country_code missing or invalid"))
        )
        .select(
            "airport_code_raw",
            "airport_name_raw",
            "city_code_raw",
            "country_code_raw",
            "latitude_raw",
            "longitude_raw",
            "location_type_raw",
            "utc_offset_raw",
            "time_zone_id_raw",
            "validation_error",
            "raw_json",
            F.col("_ingested_at").alias("source_ingestion_ts"),
            F.col("_source_file_name").alias("source_file_name"),
            "run_id",
            F.current_timestamp().alias("quarantined_ts"),
        )
    )

    if quarantine_df.take(1):
        quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

    current_df = spark.table(history_table).filter(F.col("is_current") == True)

    compare_df = (
        valid_df.alias("src")
        .join(current_df.alias("cur"), on="airport_code", how="left")
        .select(
            F.col("src.airport_code"),
            F.col("src.airport_name").alias("new_airport_name"),
            F.col("src.city_code").alias("new_city_code"),
            F.col("src.country_code").alias("new_country_code"),
            F.col("src.latitude").alias("new_latitude"),
            F.col("src.longitude").alias("new_longitude"),
            F.col("src.location_type").alias("new_location_type"),
            F.col("src.utc_offset").alias("new_utc_offset"),
            F.col("src.time_zone_id").alias("new_time_zone_id"),
            F.col("src.observed_ts"),
            F.col("src._ingested_at").alias("source_ingestion_ts"),
            F.col("src._source_file_name").alias("source_file_name"),
            F.col("src.run_id").alias("ingestion_run_id"),
            F.col("cur.airport_name").alias("current_airport_name"),
            F.col("cur.city_code").alias("current_city_code"),
            F.col("cur.country_code").alias("current_country_code"),
            F.col("cur.latitude").alias("current_latitude"),
            F.col("cur.longitude").alias("current_longitude"),
            F.col("cur.location_type").alias("current_location_type"),
            F.col("cur.utc_offset").alias("current_utc_offset"),
            F.col("cur.time_zone_id").alias("current_time_zone_id"),
        )
    )

    new_df = compare_df.filter(F.col("current_airport_name").isNull())

    unchanged_df = compare_df.filter(
        F.col("current_airport_name").isNotNull() &
        F.col("new_airport_name").eqNullSafe(F.col("current_airport_name")) &
        F.col("new_city_code").eqNullSafe(F.col("current_city_code")) &
        F.col("new_country_code").eqNullSafe(F.col("current_country_code")) &
        F.col("new_latitude").eqNullSafe(F.col("current_latitude")) &
        F.col("new_longitude").eqNullSafe(F.col("current_longitude")) &
        F.col("new_location_type").eqNullSafe(F.col("current_location_type")) &
        F.col("new_utc_offset").eqNullSafe(F.col("current_utc_offset")) &
        F.col("new_time_zone_id").eqNullSafe(F.col("current_time_zone_id"))
    )

    changed_df = compare_df.filter(
        F.col("current_airport_name").isNotNull() &
        (
            ~F.col("new_airport_name").eqNullSafe(F.col("current_airport_name")) |
            ~F.col("new_city_code").eqNullSafe(F.col("current_city_code")) |
            ~F.col("new_country_code").eqNullSafe(F.col("current_country_code")) |
            ~F.col("new_latitude").eqNullSafe(F.col("current_latitude")) |
            ~F.col("new_longitude").eqNullSafe(F.col("current_longitude")) |
            ~F.col("new_location_type").eqNullSafe(F.col("current_location_type")) |
            ~F.col("new_utc_offset").eqNullSafe(F.col("current_utc_offset")) |
            ~F.col("new_time_zone_id").eqNullSafe(F.col("current_time_zone_id"))
        )
    )

    history_dt = DeltaTable.forName(spark, history_table)

    unchanged_updates_df = unchanged_df.select(
        "airport_code",
        F.col("observed_ts").alias("new_last_seen_ts"),
    )

    if unchanged_updates_df.take(1):
        (
            history_dt.alias("t")
            .merge(
                unchanged_updates_df.alias("s"),
                "t.airport_code = s.airport_code AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "last_seen_ts": "s.new_last_seen_ts"
            })
            .execute()
        )

    changed_closures_df = changed_df.select(
        "airport_code",
        F.col("observed_ts").alias("new_observed_to_ts"),
        F.col("ingestion_run_id").alias("new_closed_by_run_id"),
    )

    if changed_closures_df.take(1):
        (
            history_dt.alias("t")
            .merge(
                changed_closures_df.alias("s"),
                "t.airport_code = s.airport_code AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "observed_to_ts": "s.new_observed_to_ts",
                "is_current": "false",
                "closed_by_run_id": "s.new_closed_by_run_id",
            })
            .execute()
        )

    inserts_df = (
        new_df.select(
            F.col("airport_code"),
            F.col("new_airport_name").alias("airport_name"),
            F.col("new_city_code").alias("city_code"),
            F.col("new_country_code").alias("country_code"),
            F.col("new_latitude").alias("latitude"),
            F.col("new_longitude").alias("longitude"),
            F.col("new_location_type").alias("location_type"),
            F.col("new_utc_offset").alias("utc_offset"),
            F.col("new_time_zone_id").alias("time_zone_id"),
            F.col("observed_ts").alias("observed_from_ts"),
            F.lit(None).cast("timestamp").alias("observed_to_ts"),
            F.col("observed_ts").alias("last_seen_ts"),
            F.lit(True).alias("is_current"),
            "source_ingestion_ts",
            "source_file_name",
            F.col("ingestion_run_id").alias("created_by_run_id"),
            F.lit(None).cast("string").alias("closed_by_run_id"),
        )
        .unionByName(
            changed_df.select(
                F.col("airport_code"),
                F.col("new_airport_name").alias("airport_name"),
                F.col("new_city_code").alias("city_code"),
                F.col("new_country_code").alias("country_code"),
                F.col("new_latitude").alias("latitude"),
                F.col("new_longitude").alias("longitude"),
                F.col("new_location_type").alias("location_type"),
                F.col("new_utc_offset").alias("utc_offset"),
                F.col("new_time_zone_id").alias("time_zone_id"),
                F.col("observed_ts").alias("observed_from_ts"),
                F.lit(None).cast("timestamp").alias("observed_to_ts"),
                F.col("observed_ts").alias("last_seen_ts"),
                F.lit(True).alias("is_current"),
                "source_ingestion_ts",
                "source_file_name",
                F.col("ingestion_run_id").alias("created_by_run_id"),
                F.lit(None).cast("string").alias("closed_by_run_id"),
            )
        )
    )

    if inserts_df.take(1):
        inserts_df.write.format("delta").mode("append").saveAsTable(history_table)

def refresh_airports_current(
    spark,
    history_table: str,
    current_table: str,
):
    """
    Rebuild the current table from the current rows in history.
    """
    current_df = (
        spark.table(history_table)
        .filter(F.col("is_current") == True)
        .select(
            "airport_code",
            "airport_name",
            "city_code",
            "country_code",
            "latitude",
            "longitude",
            "location_type",
            "utc_offset",
            "time_zone_id",
            "observed_from_ts",
            "last_seen_ts",
            "source_ingestion_ts",
            "source_file_name",
            "created_by_run_id",
        )
    )

    current_df.write.format("delta").mode("overwrite").saveAsTable(current_table)

def load_airports_to_silver(
    spark,
    bronze_table: str,
    history_table: str,
    current_table: str,
    quarantine_table: str,
    state_table: str,
):
    """
    Orchestrate the full Bronze-to-Silver airports load.
    """
    entity_name = "airports_history"

    ensure_history_table(spark, history_table)
    ensure_current_table(spark, current_table)
    ensure_quarantine_table(spark, quarantine_table)

    latest_processed_run_id = get_latest_processed_run_id(
        spark,
        state_table,
        entity_name,
    )

    bronze_runs_df = spark.table(bronze_table).select("run_id").distinct()

    if latest_processed_run_id is not None:
        bronze_runs_df = bronze_runs_df.filter(
            F.col("run_id") > latest_processed_run_id
        )

    run_ids = [
        row["run_id"]
        for row in bronze_runs_df.orderBy("run_id").collect()
    ]

    for run_id in run_ids:
        process_airports_run(
            spark=spark,
            bronze_table=bronze_table,
            history_table=history_table,
            quarantine_table=quarantine_table,
            run_id=run_id,
        )

        update_latest_processed_run_id(
            spark=spark,
            state_table=state_table,
            entity_name=entity_name,
            latest_run_id=run_id,
        )

    refresh_airports_current(
        spark=spark,
        history_table=history_table,
        current_table=current_table,
    )