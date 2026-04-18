import requests


class IngestionContext:
    def __init__(self, spark, dbutils, session: requests.Session, run_id: str, catalog: str):
        self.spark = spark
        self.dbutils = dbutils
        self.session = session
        self.run_id = run_id
        self.catalog = catalog