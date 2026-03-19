from support.api_client import create_session
from support.context import IngestionContext
from scripts.flight_status import (
    init_flight_status_ingestion,
    run_flight_status_ingestion,
)


run_id = init_flight_status_ingestion(spark)
client_id = dbutils.secrets.get(scope="lh-api", key="client_id")
client_secret = dbutils.secrets.get(scope="lh-api", key="client_secret")
session = create_session(client_id, client_secret)

ctx = IngestionContext(
    spark=spark,
    dbutils=dbutils,
    session=session,
    run_id=run_id,
)


run_flight_status_ingestion(ctx)