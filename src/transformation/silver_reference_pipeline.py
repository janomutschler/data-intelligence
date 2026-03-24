import argparse

from silver.reference.aircraft import load_aircraft_to_silver
from silver.reference.airlines import load_airlines_to_silver
from silver.reference.airports import load_airports_to_silver
from silver.reference.cities import load_cities_to_silver
from silver.reference.countries import load_countries_to_silver
from silver.common.pipeline_state import ensure_state_table

def run_reference_silver_pipeline(
    spark,
    catalog: str,
    schema: str,
):
    """
    Run all reference-data Bronze-to-Silver loads in sequence.
    """
    state_table = f"{catalog}.{schema}.silver_pipeline_state"
    ensure_state_table(spark, state_table)

    print(f"Starting reference silver pipeline in {catalog}.{schema}")

    print("Processing countries")
    load_countries_to_silver(
        spark=spark,
        bronze_table=f"{catalog}.{schema}.bronze_countries_raw",
        history_table=f"{catalog}.{schema}.silver_countries_history",
        current_table=f"{catalog}.{schema}.silver_countries_current",
        quarantine_table=f"{catalog}.{schema}.silver_countries_quarantine",
        state_table=state_table,
    )

    print("Processing aircraft")
    load_aircraft_to_silver(
        spark=spark,
        bronze_table=f"{catalog}.{schema}.bronze_aircraft_raw",
        history_table=f"{catalog}.{schema}.silver_aircraft_history",
        current_table=f"{catalog}.{schema}.silver_aircraft_current",
        quarantine_table=f"{catalog}.{schema}.silver_aircraft_quarantine",
        state_table=state_table,
    )

    print("Processing airlines")
    load_airlines_to_silver(
        spark=spark,
        bronze_table=f"{catalog}.{schema}.bronze_airlines_raw",
        history_table=f"{catalog}.{schema}.silver_airlines_history",
        current_table=f"{catalog}.{schema}.silver_airlines_current",
        quarantine_table=f"{catalog}.{schema}.silver_airlines_quarantine",
        state_table=state_table,
    )

    print("Processing airports")
    load_airports_to_silver(
        spark=spark,
        bronze_table=f"{catalog}.{schema}.bronze_airports_raw",
        history_table=f"{catalog}.{schema}.silver_airports_history",
        current_table=f"{catalog}.{schema}.silver_airports_current",
        quarantine_table=f"{catalog}.{schema}.silver_airports_quarantine",
        state_table=state_table,
    )

    print("Processing cities")
    load_cities_to_silver(
        spark=spark,
        bronze_table=f"{catalog}.{schema}.bronze_cities_raw",
        history_table=f"{catalog}.{schema}.silver_cities_history",
        current_table=f"{catalog}.{schema}.silver_cities_current",
        quarantine_table=f"{catalog}.{schema}.silver_cities_quarantine",
        state_table=state_table,
    )

    print("Reference silver pipeline finished successfully")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    run_reference_silver_pipeline(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
    )


if __name__ == "__main__":
    main()