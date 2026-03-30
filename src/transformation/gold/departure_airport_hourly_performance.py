from config import SILVER_SCHEMA, GOLD_SCHEMA
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
    comment="Hourly airport departure operations metrics aggregated from silver flight status."
)
@dp.expect_all_or_drop(GOLD_EXPECTATIONS)
def gold_departure_airport_hourly():
    silver_df = spark.read.table(SILVER_TABLE)

    is_cancelled = F.coalesce(F.col("is_cancelled"), F.lit(False))
    is_diverted = F.coalesce(F.col("is_diverted"), F.lit(False))
    is_rerouted = F.coalesce(F.col("is_rerouted"), F.lit(False))
    is_route_disrupted = is_diverted | is_rerouted
    is_operated = ~is_cancelled
    is_operated_as_planned = is_operated & ~is_route_disrupted

    relevant_flights_df = silver_df.filter(
        F.col("actual_departure_utc_ts").isNotNull()
        | F.col("actual_arrival_utc_ts").isNotNull()
        | is_cancelled
    )

    enriched_df = relevant_flights_df.withColumn(
        "departure_hour",
        F.hour("scheduled_departure_local_ts")
    )

    departure_on_time = (
        is_operated_as_planned
        & F.col("departure_delay_minutes").isNotNull()
        & (F.col("departure_delay_minutes") <= 15)
    )

    arrival_on_time = (
        is_operated_as_planned
        & F.col("arrival_delay_minutes").isNotNull()
        & (F.col("arrival_delay_minutes") <= 15)
    )

    aggregated_df = (
        enriched_df
        .groupBy(
            "flight_date",
            "departure_airport_code",
            "departure_hour",
        )
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
            F.sum(F.when(departure_on_time, 1).otherwise(0)).alias("on_time_departures"),
            F.sum(F.when(arrival_on_time, 1).otherwise(0)).alias("on_time_arrivals"),
        )
    )

    result_df = (
        aggregated_df
        .withColumn(
            "operated_flights",
            F.col("total_flights") - F.col("cancelled_flights")
        )
        .withColumn(
            "operated_as_planned_flights",
            F.col("operated_flights") - F.col("route_disrupted_flights")
        )
        .withColumn(
            "departure_otp_15_pct",
            F.when(
                F.col("operated_as_planned_flights") > 0,
                F.col("on_time_departures") / F.col("operated_as_planned_flights"),
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "arrival_otp_15_pct",
            F.when(
                F.col("operated_as_planned_flights") > 0,
                F.col("on_time_arrivals") / F.col("operated_as_planned_flights"),
            ).otherwise(F.lit(0.0))
        )
    )

    return result_df