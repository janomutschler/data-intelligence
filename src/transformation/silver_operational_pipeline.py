import argparse

from silver.common.pipeline_state import ensure_state_table
from silver.operational.operational_data import process_operational_data


def run_operational_silver_pipeline(
    spark,
    catalog: str,
    schema: str,
):
    """
    Run all operational-data Bronze-to-Silver loads in sequence.
    """
    state_table = f"{catalog}.{schema}.silver_pipeline_state"
    ensure_state_table(spark, state_table)

    print(f"Starting operational silver pipeline in {catalog}.{schema}")

    print("Processing flight_status")
    process_operational_data(
        spark=spark,
        bronze_table=f"{catalog}.{schema}.bronze_flight_status_raw",
        silver_table=f"{catalog}.{schema}.silver_flight_status",
        quarantine_table=f"{catalog}.{schema}.silver_flight_status_quarantine",
        state_table=state_table,
        entity_name="flight_status",
    )

    print("Processing schedules")
    process_operational_data(
        spark=spark,
        bronze_table=f"{catalog}.{schema}.bronze_schedules_raw",
        silver_table=f"{catalog}.{schema}.silver_schedules",
        quarantine_table=f"{catalog}.{schema}.silver_schedules_quarantine",
        state_table=state_table,
        entity_name="schedules",
    )

    print("Operational silver pipeline finished successfully")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    run_operational_silver_pipeline(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
    )


if __name__ == "__main__":
    main()