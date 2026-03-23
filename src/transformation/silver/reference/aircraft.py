from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from silver.common.pipeline_state import (
    get_latest_processed_run_id,
    update_latest_processed_run_id,
)

aircraft_array_schema = StructType([
    StructField("AircraftResource", StructType([
        StructField("AircraftSummaries", StructType([
            StructField("AircraftSummary", ArrayType(
                StructType([
                    StructField("AircraftCode", StringType(), True),
                    StructField("Names", StructType([
                        StructField("Name", StructType([
                            StructField("@LanguageCode", StringType(), True),
                            StructField("$", StringType(), True),
                        ]), True)
                    ]), True),
                    StructField("AirlineEquipCode", StringType(), True),
                ])
            ), True)
        ]), True)
    ]), True)
])

aircraft_single_schema = StructType([
    StructField("AircraftResource", StructType([
        StructField("AircraftSummaries", StructType([
            StructField("AircraftSummary", StructType([
                StructField("AircraftCode", StringType(), True),
                StructField("Names", StructType([
                    StructField("Name", StructType([
                        StructField("@LanguageCode", StringType(), True),
                        StructField("$", StringType(), True),
                    ]), True)
                ]), True),
                StructField("AirlineEquipCode", StringType(), True),
            ]), True)
        ]), True)
    ]), True)
])

def ensure_history_table(spark, history_table: str):
    if not spark.catalog.tableExists(history_table):
        df = spark.createDataFrame(
            [],
            """
            aircraft_code string,
            aircraft_name string,
            airline_equip_code string,
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
            aircraft_code string,
            aircraft_name string,
            airline_equip_code string,
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
            aircraft_code_raw string,
            aircraft_name_raw string,
            airline_equip_code_raw string,
            validation_error string,
            raw_json string,
            source_ingestion_ts timestamp,
            source_file_name string,
            run_id string,
            quarantined_ts timestamp
            """
        )
        df.write.format("delta").mode("overwrite").saveAsTable(quarantine_table)

def process_aircraft_run(
    spark,
    bronze_table: str,
    history_table: str,
    quarantine_table: str,
    run_id: str,
):
    """
    Loads one run of the aircraft bronze table and processes it to silver history.
    """
    bronze_df = spark.table(bronze_table).filter(F.col("run_id") == run_id)

    parsed_array_df = bronze_df.withColumn(
        "parsed_array_json",
        F.from_json(F.col("raw_json"), aircraft_array_schema)
    )

    parsed_both_df = parsed_array_df.withColumn(
        "parsed_single_json",
        F.from_json(F.col("raw_json"), aircraft_single_schema)
    )

    normalized_df = parsed_both_df.withColumn(
        "aircraft_array",
        F.when(
            F.col("parsed_array_json.AircraftResource.AircraftSummaries.AircraftSummary").isNotNull(),
            F.col("parsed_array_json.AircraftResource.AircraftSummaries.AircraftSummary")
        ).otherwise(
            F.array(F.col("parsed_single_json.AircraftResource.AircraftSummaries.AircraftSummary"))
        )
    )

    exploded_df = normalized_df.withColumn(
        "aircraft",
        F.explode_outer(F.col("aircraft_array"))
    )

    staged_df = (
        exploded_df
        .select(
            F.col("aircraft.AircraftCode").alias("aircraft_code_raw"),
            F.col("aircraft.Names.Name.$").alias("aircraft_name_raw"),
            F.col("aircraft.AirlineEquipCode").alias("airline_equip_code_raw"),
            F.col("raw_json"),
            F.col("run_id"),
            F.col("_source_file_name"),
            F.col("_source_file_modification_time"),
            F.col("_ingested_at"),
        )
        .withColumn("aircraft_code", F.upper(F.trim(F.col("aircraft_code_raw"))))
        .withColumn("aircraft_name", F.trim(F.col("aircraft_name_raw")))
        .withColumn("airline_equip_code", F.upper(F.trim(F.col("airline_equip_code_raw"))))
        .withColumn(
            "observed_ts",
            F.coalesce(F.col("_source_file_modification_time"), F.col("_ingested_at"))
        )
    )

    invalid_df = staged_df.filter(
        F.col("aircraft_code").isNull() | (F.col("aircraft_code") == "") |
        F.col("aircraft_name").isNull() | (F.col("aircraft_name") == "")
    )

    valid_df = (
        staged_df
        .filter(
            F.col("aircraft_code").isNotNull() & (F.col("aircraft_code") != "") &
            F.col("aircraft_name").isNotNull() & (F.col("aircraft_name") != "")
        )
        .dropDuplicates(["aircraft_code"])
    )

    quarantine_df = (
        invalid_df
        .withColumn(
            "validation_error",
            F.when(
                F.col("aircraft_code").isNull() | (F.col("aircraft_code") == ""),
                F.lit("aircraft_code missing or invalid")
            ).otherwise(F.lit("aircraft_name missing or invalid"))
        )
        .select(
            "aircraft_code_raw",
            "aircraft_name_raw",
            "airline_equip_code_raw",
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
        .join(current_df.alias("cur"), on="aircraft_code", how="left")
        .select(
            F.col("src.aircraft_code"),
            F.col("src.aircraft_name").alias("new_aircraft_name"),
            F.col("src.airline_equip_code").alias("new_airline_equip_code"),
            F.col("src.observed_ts"),
            F.col("src._ingested_at").alias("source_ingestion_ts"),
            F.col("src._source_file_name").alias("source_file_name"),
            F.col("src.run_id").alias("ingestion_run_id"),
            F.col("cur.aircraft_name").alias("current_aircraft_name"),
            F.col("cur.airline_equip_code").alias("current_airline_equip_code"),
        )
    )

    new_df = compare_df.filter(F.col("current_aircraft_name").isNull())

    unchanged_df = compare_df.filter(
        F.col("current_aircraft_name").isNotNull() &
        F.col("new_aircraft_name").eqNullSafe(F.col("current_aircraft_name")) &
        F.col("new_airline_equip_code").eqNullSafe(F.col("current_airline_equip_code"))
    )

    changed_df = compare_df.filter(
        F.col("current_aircraft_name").isNotNull() &
        (
            ~F.col("new_aircraft_name").eqNullSafe(F.col("current_aircraft_name")) |
            ~F.col("new_airline_equip_code").eqNullSafe(F.col("current_airline_equip_code"))
        )
    )

    history_dt = DeltaTable.forName(spark, history_table)

    unchanged_updates_df = unchanged_df.select(
        "aircraft_code",
        F.col("observed_ts").alias("new_last_seen_ts"),
    )

    if unchanged_updates_df.take(1):
        (
            history_dt.alias("t")
            .merge(
                unchanged_updates_df.alias("s"),
                "t.aircraft_code = s.aircraft_code AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "last_seen_ts": "s.new_last_seen_ts"
            })
            .execute()
        )

    changed_closures_df = changed_df.select(
        "aircraft_code",
        F.col("observed_ts").alias("new_observed_to_ts"),
        F.col("ingestion_run_id").alias("new_closed_by_run_id"),
    )

    if changed_closures_df.take(1):
        (
            history_dt.alias("t")
            .merge(
                changed_closures_df.alias("s"),
                "t.aircraft_code = s.aircraft_code AND t.is_current = true"
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
            F.col("aircraft_code"),
            F.col("new_aircraft_name").alias("aircraft_name"),
            F.col("new_airline_equip_code").alias("airline_equip_code"),
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
                F.col("aircraft_code"),
                F.col("new_aircraft_name").alias("aircraft_name"),
                F.col("new_airline_equip_code").alias("airline_equip_code"),
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

def refresh_aircraft_current(
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
            "aircraft_code",
            "aircraft_name",
            "airline_equip_code",
            "observed_from_ts",
            "last_seen_ts",
            "source_ingestion_ts",
            "source_file_name",
            "created_by_run_id",
        )
    )

    current_df.write.format("delta").mode("overwrite").saveAsTable(current_table)

def load_aircraft_to_silver(
    spark,
    bronze_table: str,
    history_table: str,
    current_table: str,
    quarantine_table: str,
    state_table: str,
):
    """
    Orchestrate the full Bronze-to-Silver aircraft load.
    """
    entity_name = "aircraft_history"

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
        process_aircraft_run(
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

    refresh_aircraft_current(
        spark=spark,
        history_table=history_table,
        current_table=current_table,
    )