from pyspark import pipelines as dp
from pyspark.sql import functions as F
from config import SILVER_SCHEMA, GOLD_SCHEMA

CATALOG = spark.conf.get("catalog")
SILVER_FLIGHT_STATUS_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.flight_status_current"
SILVER_AIRPORTS_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.airports_current"
GOLD_DISTANCE_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.airport_distance_category_daily_performance"

DISTANCE_EXPECTATIONS = {
    "valid_distance_category": "distance_category IS NOT NULL",
    "non_negative_total_flights": "total_flights >= 0",
    "non_negative_cancelled_flights": "cancelled_flights >= 0",
    "non_negative_route_disrupted_flights": "route_disrupted_flights >= 0",
    "non_negative_operated_flights": "operated_flights >= 0",
    "non_negative_operated_as_planned_flights": "operated_as_planned_flights >= 0",
    "valid_departure_otp_range": "departure_otp_15_pct BETWEEN 0 AND 1",
    "valid_arrival_otp_range": "arrival_otp_15_pct BETWEEN 0 AND 1",
}


@dp.temporary_view(name="flight_status_airports_enriched_tmp")
def flight_status_airports_enriched_tmp():
    """
    Enrich flight status records with departure and arrival airport coordinates.
    """
    flights_df = spark.read.table(SILVER_FLIGHT_STATUS_TABLE)

    dep_airports_df = (
        spark.read.table(SILVER_AIRPORTS_TABLE)
        .select(
            F.col("airport_code").alias("dep_airport_code_ref"),
            F.col("latitude").alias("dep_latitude"),
            F.col("longitude").alias("dep_longitude"),
        )
    )

    arr_airports_df = (
        spark.read.table(SILVER_AIRPORTS_TABLE)
        .select(
            F.col("airport_code").alias("arr_airport_code_ref"),
            F.col("latitude").alias("arr_latitude"),
            F.col("longitude").alias("arr_longitude"),
        )
    )

    return (
        flights_df
        .join(
            dep_airports_df,
            flights_df["departure_airport_code"] == dep_airports_df["dep_airport_code_ref"],
            "left",
        )
        .join(
            arr_airports_df,
            flights_df["arrival_airport_code"] == arr_airports_df["arr_airport_code_ref"],
            "left",
        )
    )


@dp.temporary_view(name="flight_status_distance_tmp")
def flight_status_distance_tmp():
    """
    Calculate route distance in kilometers and derive a haul category.
    """
    df = spark.read.table("flight_status_airports_enriched_tmp")

    df = (
        df
        .withColumn("dep_lat_rad", F.radians("dep_latitude"))
        .withColumn("dep_lon_rad", F.radians("dep_longitude"))
        .withColumn("arr_lat_rad", F.radians("arr_latitude"))
        .withColumn("arr_lon_rad", F.radians("arr_longitude"))
    )

    distance_expr = (
        2 * F.lit(6371.0) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.col("arr_lat_rad") - F.col("dep_lat_rad")) / 2), 2)
                + F.cos(F.col("dep_lat_rad"))
                * F.cos(F.col("arr_lat_rad"))
                * F.pow(F.sin((F.col("arr_lon_rad") - F.col("dep_lon_rad")) / 2), 2)
            )
        )
    )

    return (
        df
        .withColumn(
            "distance_km",
            F.when(
                F.col("dep_latitude").isNotNull()
                & F.col("dep_longitude").isNotNull()
                & F.col("arr_latitude").isNotNull()
                & F.col("arr_longitude").isNotNull(),
                distance_expr,
            )
        )
        .withColumn(
            "distance_category",
            F.when(F.col("distance_km") < 1500, F.lit("short_haul"))
             .when(F.col("distance_km") < 3500, F.lit("medium_haul"))
             .when(F.col("distance_km") >= 3500, F.lit("long_haul"))
        )
    )


@dp.table(
    name=GOLD_DISTANCE_TABLE,
    comment="Daily operational performance KPIs by departure airport and distance category."
)
@dp.expect_all_or_drop(DISTANCE_EXPECTATIONS)
def gold_airport_distance_category_daily_performance():
    """
    Compare short-, medium-, and long-haul operational performance
    within each departure airport.
    """
    silver_df = spark.read.table("flight_status_distance_tmp")

    is_cancelled = F.coalesce(F.col("is_cancelled"), F.lit(False))
    is_diverted = F.coalesce(F.col("is_diverted"), F.lit(False))
    is_rerouted = F.coalesce(F.col("is_rerouted"), F.lit(False))
    is_route_disrupted = is_diverted | is_rerouted
    is_operated = ~is_cancelled
    is_operated_as_planned = is_operated & ~is_route_disrupted

    relevant_flights_df = (
        silver_df
        .filter(F.col("distance_category").isNotNull())
        .filter(F.col("departure_airport_code").isNotNull())
        .filter(
            F.col("actual_departure_utc_ts").isNotNull()
            | F.col("actual_arrival_utc_ts").isNotNull()
            | is_cancelled
        )
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
            "distance_category",
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

    return (
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
        .withColumn(
			"cancellation_rate",
			F.when(F.col("total_flights") > 0,
				F.col("cancelled_flights") / F.col("total_flights"))
			.otherwise(F.lit(0.0))
		)
		.withColumn(
			"route_disruption_rate",
			F.when(F.col("operated_flights") > 0,
				F.col("route_disrupted_flights") / F.col("operated_flights"))
			.otherwise(F.lit(0.0))
		)
	)