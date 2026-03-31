from common.api_client import create_session
from common.context import IngestionContext
from scripts.reference_data import run_reference_ingestion, init_reference_ingestion

def main():
    run_id = init_reference_ingestion(spark)
    client_id = dbutils.secrets.get(scope="lh-api", key="client_id")
    client_secret = dbutils.secrets.get(scope="lh-api", key="client_secret")
    session = create_session(client_id, client_secret)

    ctx = IngestionContext(
        spark=spark,
        dbutils=dbutils,
        session=session,
        run_id=run_id,
    )

    run_reference_ingestion(ctx)

if __name__ == "__main__":
    main()