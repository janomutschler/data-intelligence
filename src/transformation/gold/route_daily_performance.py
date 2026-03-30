from pyspark import pipelines as dp
from pyspark.sql import functions as F
from config import SILVER_SCHEMA, GOLD_SCHEMA

CATALOG = spark.conf.get("catalog")
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.flight_status_current"
GOLD_ROUTE_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.route_daily_performance"


ROUTE_EXPECTATIONS = {
    "valid_flight_date": "flight_date IS NOT NULL",
    "valid_departure_airport_code": "departure_airport_code IS NOT NULL",
    "valid_arrival_airport_code": "arrival_airport_code IS NOT NULL",
    "non_negative_total_flights": "total_flights >= 0",
    "non_negative_cancelled_flights": "cancelled_flights >= 0",
    "non_negative_route_disrupted_flights": "route_disrupted_flights >= 0",
    "non_negative_operated_flights": "operated_flights >= 0",
    "non_negative_operated_as_planned_flights": "operated_as_planned_flights >= 0",
    "valid_departure_otp_range": "departure_otp_15_pct BETWEEN 0 AND 1",
    "valid_arrival_otp_range": "arrival_otp_15_pct BETWEEN 0 AND 1",
}


@dp.table(
    name=GOLD_ROUTE_TABLE,
    comment="Daily operational performance KPIs by route."
)
@dp.expect_all_or_drop(ROUTE_EXPECTATIONS)
def gold_route_daily_performance():
    """
    Aggregate daily operational performance by departure-arrival route.

    The table is intended for route reliability analysis and compares
    cancellations, route disruptions, delays, and OTP across airport pairs.
    """
    silver_df = spark.read.table(SILVER_TABLE)

    is_cancelled = F.coalesce(F.col("is_cancelled"), F.lit(False))
    is_diverted = F.coalesce(F.col("is_diverted"), F.lit(False))
    is_rerouted = F.coalesce(F.col("is_rerouted"), F.lit(False))
    is_route_disrupted = is_diverted | is_rerouted
    is_operated = ~is_cancelled
    is_operated_as_planned = is_operated & ~is_route_disrupted

    relevant_flights_df = silver_df.filter(
        (
            F.col("actual_departure_utc_ts").isNotNull()
            | F.col("actual_arrival_utc_ts").isNotNull()
            | is_cancelled
        )
        & F.col("flight_date").isNotNull()
        & F.col("departure_airport_code").isNotNull()
        & F.col("arrival_airport_code").isNotNull()
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
        relevant_flights_df
        .groupBy(
            "flight_date",
            "departure_airport_code",
            "arrival_airport_code",
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
            "route",
            F.concat_ws(" ->", F.col("departure_airport_code"), F.col("arrival_airport_code"))
        )
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
        .withColumn(
            "cancellation_rate",
            F.when(
                F.col("total_flights") > 0,
                F.col("cancelled_flights") / F.col("total_flights"),
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "route_disruption_rate",
            F.when(
                F.col("operated_flights") > 0,
                F.col("route_disrupted_flights") / F.col("operated_flights"),
            ).otherwise(F.lit(0.0))
        )
    )

    return result_df