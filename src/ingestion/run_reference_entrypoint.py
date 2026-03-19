import support.api_client as api
import support.context as context
import scripts.reference_data as reference_data

run_id = reference_data.init_reference_data_ingestion(spark)
client_id = dbutils.secrets.get(scope="lh-api", key="client_id")
client_secret = dbutils.secrets.get(scope="lh-api", key="client_secret")
session = api.create_session(client_id, client_secret)

ctx = context.IngestionContext(
    spark=spark,
    dbutils=dbutils,
    session=session,
    run_id=run_id,
)

reference_data.run_reference_data_ingestion_all(ctx)