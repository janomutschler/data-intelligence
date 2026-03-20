from support.api_client import create_session
from support.context import IngestionContext
from scripts.schedules import (
    init_schedules_ingestion,
    run_schedules_ingestion,
)


run_id = init_schedules_ingestion(spark)
client_id = dbutils.secrets.get(scope="lh-api", key="client_id")
client_secret = dbutils.secrets.get(scope="lh-api", key="client_secret")
session = create_session(client_id, client_secret)

ctx = IngestionContext(
    spark=spark,
    dbutils=dbutils,
    session=session,
    run_id=run_id,
)


run_schedules_ingestion(ctx)