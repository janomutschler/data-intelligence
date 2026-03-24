import argparse

from gold.departure_airport_hourly import build_gold_departure_airport_hourly


def run_gold_pipeline(
    spark,
    catalog: str,
    schema: str,
):
    print(f"Starting gold pipeline in {catalog}.{schema}")

    build_gold_departure_airport_hourly(
        spark=spark,
        silver_table=f"{catalog}.{schema}.silver_flight_status",
        gold_table=f"{catalog}.{schema}.gold_departure_airport_hourly",
    )

    print("Gold pipeline finished successfully")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    run_gold_pipeline(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
    )


if __name__ == "__main__":
    main()