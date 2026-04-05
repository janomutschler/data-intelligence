from config import GOLD_SCHEMA, SILVER_SCHEMA
from gold.common import build_flight_flags, flight_flag_cols
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")

SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.flight_status_current"
TARGET_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.departure_airport_hourly"

GOLD_EXPECTATIONS = {
    "valid_flight_date": "flight_date IS NOT NULL",
    "valid_airport": "departure_airport_code IS NOT NULL AND TRIM(departure_airport_code) <> ''",
    "valid_hour": "departure_hour BETWEEN 0 AND 23",
    "non_negative_flights": "total_flights >= 0",
    "valid_departure_otp_range": "departure_otp_15_pct BETWEEN 0 AND 1",
    "valid_arrival_otp_range": "arrival_otp_15_pct BETWEEN 0 AND 1",
}


@dp.materialized_view(
    name=TARGET_TABLE,
    comment="Hourly airport departure operations metrics aggregated from silver flight status.",
)
@dp.expect_all_or_drop(GOLD_EXPECTATIONS)
def gold_departure_airport_hourly():
    silver_df = spark.read.table(SILVER_TABLE)
    flagged_df = build_flight_flags(silver_df)
    is_cancelled, is_route_disrupted, _is_operated, is_operated_as_planned = flight_flag_cols()

    relevant_flights_df = flagged_df.filter(
        F.col("actual_departure_utc_ts").isNotNull()
        | F.col("actual_arrival_utc_ts").isNotNull()
        | is_cancelled
    )

    enriched_df = relevant_flights_df.withColumn(
        "departure_hour",
        F.hour("scheduled_departure_local_ts"),
    )

    departure_on_time_or_early = (
        is_operated_as_planned
        & F.col("departure_delay_minutes").isNotNull()
        & (F.col("departure_delay_minutes") <= 15)
    )
    arrival_on_time = (
        is_operated_as_planned
        & F.col("arrival_delay_minutes").isNotNull()
        & (F.col("arrival_delay_minutes") <= 15)
    )
    departure_early = is_operated_as_planned & (F.col("departure_delay_minutes") < 0)
    departure_on_time_0_15 = (
        is_operated_as_planned
        & (F.col("departure_delay_minutes") >= 0)
        & (F.col("departure_delay_minutes") <= 15)
    )
    departure_delay_15_60 = (
        is_operated_as_planned
        & (F.col("departure_delay_minutes") > 15)
        & (F.col("departure_delay_minutes") <= 60)
    )
    departure_delay_60_120 = (
        is_operated_as_planned
        & (F.col("departure_delay_minutes") > 60)
        & (F.col("departure_delay_minutes") <= 120)
    )
    departure_delay_120_plus = is_operated_as_planned & (F.col("departure_delay_minutes") > 120)

    aggregated_df = (
        enriched_df
        .groupBy("flight_date", "departure_airport_code", "departure_hour")
        .agg(
            F.count("*").alias("total_flights"),
            F.sum(is_cancelled.cast("int")).alias("cancelled_flights"),
            F.sum(is_route_disrupted.cast("int")).alias("route_disrupted_flights"),
            F.coalesce(
                F.avg(F.when(is_operated_as_planned, F.col("departure_delay_minutes"))),
                F.lit(0.0),
            ).alias("avg_departure_delay_minutes"),
            F.coalesce(
                F.avg(F.when(is_operated_as_planned, F.col("arrival_delay_minutes"))),
                F.lit(0.0),
            ).alias("avg_arrival_delay_minutes"),
            F.sum(F.when(departure_on_time_or_early, 1).otherwise(0)).alias("on_time_departures"),
            F.sum(F.when(arrival_on_time, 1).otherwise(0)).alias("on_time_arrivals"),
            F.sum(F.when(departure_early, 1).otherwise(0)).alias("early_departures"),
            F.sum(F.when(departure_on_time_0_15, 1).otherwise(0)).alias("on_time_0_15_departures"),
            F.sum(F.when(departure_delay_15_60, 1).otherwise(0)).alias("delay_15_60_departures"),
            F.sum(F.when(departure_delay_60_120, 1).otherwise(0)).alias("delay_60_120_departures"),
            F.sum(F.when(departure_delay_120_plus, 1).otherwise(0)).alias("delay_120_plus_departures"),
        )
    )

    return (
        aggregated_df
        .withColumn("operated_flights", F.col("total_flights") - F.col("cancelled_flights"))
        .withColumn(
            "operated_as_planned_flights",
            F.col("operated_flights") - F.col("route_disrupted_flights"),
        )
        .withColumn(
            "departure_otp_15_pct",
            F.when(
                F.col("operated_as_planned_flights") > 0,
                F.col("on_time_departures") / F.col("operated_as_planned_flights"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "arrival_otp_15_pct",
            F.when(
                F.col("operated_as_planned_flights") > 0,
                F.col("on_time_arrivals") / F.col("operated_as_planned_flights"),
            ).otherwise(F.lit(0.0)),
        )
    )
