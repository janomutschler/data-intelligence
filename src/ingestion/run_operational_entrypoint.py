import argparse

from common.api_client import create_session
from common.context import IngestionContext
from common.utils import configure_logging
from scripts.operational_data import init_operational_ingestion, run_flight_status_ingestion


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    catalog: str = args.catalog
    run_id = init_operational_ingestion(spark, catalog)

    client_id = dbutils.secrets.get(scope="lh-api", key="client_id")
    client_secret = dbutils.secrets.get(scope="lh-api", key="client_secret")
    session = create_session(client_id, client_secret)

    ctx = IngestionContext(
        spark=spark,
        dbutils=dbutils,
        session=session,
        run_id=run_id,
        catalog=catalog,
    )

    run_flight_status_ingestion(ctx)


if __name__ == "__main__":
    main()