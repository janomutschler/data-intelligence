from pyspark.sql.functions import col, current_timestamp



def raw_table_df(spark, source_path: str):
    """
    Reads raw ingestion files from a Unity Catalog volume using Databricks Auto Loader (cloudFiles)
    and returns a streaming DataFrame for the Bronze layer.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .load(source_path)
        .withColumnRenamed("value", "raw_json")
        .withColumn("_source_file_path", col("_metadata.file_path"))
        .withColumn("_source_file_name", col("_metadata.file_name"))
        .withColumn("_source_file_size", col("_metadata.file_size"))
        .withColumn("_source_file_modification_time", col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", current_timestamp())
    )


