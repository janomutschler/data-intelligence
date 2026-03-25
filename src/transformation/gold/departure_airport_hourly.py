from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
)


def ensure_gold_departure_airport_hourly_table(spark, gold_table: str):
    if not spark.catalog.tableExists(gold_table):
        schema = StructType([
            StructField("flight_date", DateType(), True),
            StructField("departure_airport_code", StringType(), True),
            StructField("departure_hour", IntegerType(), True),
            StructField("total_flights", LongType(), True),
            StructField("cancelled_flights", LongType(), True),
            StructField("avg_departure_delay_minutes", DoubleType(), True),
            StructField("avg_arrival_delay_minutes", DoubleType(), True),
            StructField("on_time_departures", LongType(), True),
            StructField("on_time_arrivals", LongType(), True),
            StructField("operated_flights", LongType(), True),
            StructField("departure_otp_15_pct", DoubleType(), True),
            StructField("arrival_otp_15_pct", DoubleType(), True),
        ])

        empty_df = spark.createDataFrame([], schema)

        (
            empty_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_table)
        )

def build_gold_departure_airport_hourly(
    spark,
    silver_table: str,
    gold_table: str,
):
    ensure_gold_departure_airport_hourly_table(spark, gold_table)
    gold_df = (
        spark.table(silver_table)
        .filter(
            (F.col("actual_departure_utc_ts").isNotNull()) |
            (F.col("actual_arrival_utc_ts").isNotNull()) |
            (F.col("is_cancelled") == True)
        )
        .withColumn(
            "departure_hour",
            F.hour("scheduled_departure_local_ts")
        )
        .groupBy(
            "flight_date",
            "departure_airport_code",
            "departure_hour"
        )
        .agg(
            F.count("*").alias("total_flights"),

            F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_flights"),

            F.coalesce(
                F.avg(
                    F.when(~F.col("is_cancelled"), F.col("departure_delay_minutes"))
                ),
                F.lit(0)
            ).alias("avg_departure_delay_minutes"),

            F.coalesce(
                F.avg(
                    F.when(~F.col("is_cancelled"), F.col("arrival_delay_minutes"))
                ),
                F.lit(0)
            ).alias("avg_arrival_delay_minutes"),

            F.sum(
                F.when(
                    (~F.col("is_cancelled")) &
                    F.col("departure_delay_minutes").isNotNull() &
                    (F.col("departure_delay_minutes") <= 15),
                    1
                ).otherwise(0)
            ).alias("on_time_departures"),

            F.sum(
                F.when(
                    (~F.col("is_cancelled")) &
                    F.col("arrival_delay_minutes").isNotNull() &
                    (F.col("arrival_delay_minutes") <= 15),
                    1
                ).otherwise(0)
            ).alias("on_time_arrivals"),
        )
        .withColumn(
            "operated_flights",
            F.col("total_flights") - F.col("cancelled_flights")
        )
        .withColumn(
            "departure_otp_15_pct",
            F.when(
                F.col("operated_flights") > 0,
                F.col("on_time_departures") / F.col("operated_flights")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "arrival_otp_15_pct",
            F.when(
                F.col("operated_flights") > 0,
                F.col("on_time_arrivals") / F.col("operated_flights")
            ).otherwise(F.lit(0.0))
        )
    )

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_table)
    )