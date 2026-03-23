from pyspark.sql import functions as F
from delta.tables import DeltaTable

def ensure_state_table(spark, state_table: str):
    if not spark.catalog.tableExists(state_table):
        df = spark.createDataFrame(
            [],
            "entity_name string, latest_run_id_processed string, updated_at timestamp"
        )
        df.write.format("delta").mode("overwrite").saveAsTable(state_table)


def get_latest_processed_run_id(spark, state_table: str, entity_name: str):
    """
    Read the latest successfully processed Bronze run_id for the given entity.
    """
    state_df = spark.table(state_table).filter(F.col("entity_name") == entity_name)

    rows = state_df.select("latest_run_id_processed").collect()
    if not rows:
        return None

    return rows[0]["latest_run_id_processed"]


def update_latest_processed_run_id(
    spark,
    state_table: str,
    entity_name: str,
    latest_run_id: str,
):
    """
    Updates the latest successfully processed Bronze run_id into the state table.
    For the given entity.
    """

    state_dt = DeltaTable.forName(spark, state_table)

    source_df = spark.createDataFrame(
        [(entity_name, latest_run_id)],
        ["entity_name", "latest_run_id_processed"]
    ).withColumn("updated_at", F.current_timestamp())

    (
        state_dt.alias("t")
        .merge(source_df.alias("s"), "t.entity_name = s.entity_name")
        .whenMatchedUpdate(set={
            "latest_run_id_processed": "s.latest_run_id_processed",
            "updated_at": "s.updated_at",
        })
        .whenNotMatchedInsert(values={
            "entity_name": "s.entity_name",
            "latest_run_id_processed": "s.latest_run_id_processed",
            "updated_at": "s.updated_at",
        })
        .execute()
    )