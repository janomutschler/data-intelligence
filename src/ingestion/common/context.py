class IngestionContext:
    def __init__(self, spark, dbutils, session, run_id: str):
        self.spark = spark
        self.dbutils = dbutils
        self.session = session
        self.run_id = run_id