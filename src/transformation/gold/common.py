"""
Shared business logic for Gold-layer aggregations.
Centralizing the operational flight flag definitions
"""
from pyspark.sql import Column
from pyspark.sql import functions as F


def build_flight_flags(df):
    """
    Derive the standard operational flight flags used across all Gold tables.

    Returns the DataFrame enriched with:
        is_cancelled        — flight was cancelled
        is_diverted         — flight was diverted to a different airport
        is_rerouted         — flight was rerouted
        is_route_disrupted  — diverted OR rerouted
        is_operated         — not cancelled
        is_operated_as_planned — operated AND not disrupted
    """
    is_cancelled: Column = F.coalesce(F.col("is_cancelled"), F.lit(False))
    is_diverted: Column = F.coalesce(F.col("is_diverted"), F.lit(False))
    is_rerouted: Column = F.coalesce(F.col("is_rerouted"), F.lit(False))
    is_route_disrupted: Column = is_diverted | is_rerouted
    is_operated: Column = ~is_cancelled
    is_operated_as_planned: Column = is_operated & ~is_route_disrupted

    return (
        df
        .withColumn("_is_cancelled", is_cancelled)
        .withColumn("_is_route_disrupted", is_route_disrupted)
        .withColumn("_is_operated", is_operated)
        .withColumn("_is_operated_as_planned", is_operated_as_planned)
    )


def flight_flag_cols() -> tuple:
    """
    Return the four derived flag columns as a tuple for use in aggregations.
    Must be called after build_flight_flags().
    """
    is_cancelled = F.col("_is_cancelled")
    is_route_disrupted = F.col("_is_route_disrupted")
    is_operated = F.col("_is_operated")
    is_operated_as_planned = F.col("_is_operated_as_planned")
    return is_cancelled, is_route_disrupted, is_operated, is_operated_as_planned
