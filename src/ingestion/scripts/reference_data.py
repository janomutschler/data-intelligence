"""
Handles ingestion of reference data from Lufthansa API
and stores raw JSON into Unity Catalog volumes.
"""
import json
import time
from functools import partial

from config.settings import (
    BASE_URL,
    SLEEP_SECONDS,
)

from support.api_client import request_with_retry
from support.utils import utc_now_str
from support.paths import reference_data_directory
from support.storage import mkdirs, write_json
from support.logging import (
    create_reference_data_log_table,
    append_reference_data_log,
)

LIMIT = 100

REFERENCE_CONFIG = {
    "countries": {
        "resource_name": "CountryResource",
        "params": {
            "lang": "en",
        },
    },
    "aircraft": {
        "resource_name": "AircraftResource",
        "params": {},
    },
    "airlines": {
        "resource_name": "AirlineResource",
        "params": {
            "lang": "en",
        },
    },
    "airports": {
        "resource_name": "AirportResource",
        "params": {
            "lang": "en",
            "LHoperated": "1",
        },
    },
    "cities": {
        "resource_name": "CityResource",
        "params": {
            "lang": "en",
        },
    },
}


def fetch_reference_pages(
    ctx,
    reference_type: str,
) -> int:
    """
    Fetch all paginated pages for one reference dataset.
    Returns the number of successfully saved pages.
    """
    log_func = partial(append_reference_data_log, ctx.spark, ctx.run_id)

    offset = 0
    page = 0
    successful_pages = 0
    reference_date = utc_now_str()[:10]

    reference_type_config = REFERENCE_CONFIG[reference_type]
    resource_name = reference_type_config["resource_name"]

    while True:
        url = f"{BASE_URL}/v1/references/{reference_type}"
        params = {
            "limit": LIMIT,
            "offset": offset,
            **reference_type_config["params"],
        }

        directory = reference_data_directory(
            reference_type=reference_type,
            reference_date=reference_date,
            run_id=ctx.run_id,
        )
        file_path = f"{directory}/page={page}.json"
        mkdirs(directory, ctx.dbutils)

        context = {
            "timestamp_utc": utc_now_str(),
            "reference_type": reference_type,
            "reference_date": reference_date,
            "page": page,
            "offset": offset,
            "file_path": file_path,
        }

        print(
            f"[reference] type={reference_type} "
            f"page={page} offset={offset}"
        )

        response = request_with_retry(
            session=ctx.session,
            url=url,
            params=params,
            context=context,
            log_func=log_func,
        )

        if response is None:
            print("Request failed after retries.")
            break

        if response.status_code != 200:
            print(f"Request failed: {response.status_code} {response.text[:300]}")
            break

        try:
            data = response.json()
        except ValueError:
            print("Invalid JSON response.")
            log_func(
                {
                    "timestamp_utc": utc_now_str(),
                    "status": "failed_invalid_json",
                    "http_status": response.status_code,
                    "url": url,
                    "params": params,
                    **context,
                }
            )
            break

        total_count = (
            data.get(resource_name, {})
            .get("Meta", {})
            .get("TotalCount")
        )

        if total_count is None:
            print("Missing TotalCount, stopping pagination.")
            log_func(
                {
                    "timestamp_utc": utc_now_str(),
                    "status": "failed_missing_totalcount",
                    "http_status": response.status_code,
                    "url": url,
                    "params": params,
                    **context,
                }
            )
            break

        write_json(
            file_path,
            json.dumps(data, indent=2, ensure_ascii=False),
            ctx.dbutils,
        )

        log_func(
            {
                "timestamp_utc": utc_now_str(),
                "status": "success",
                "http_status": response.status_code,
                "url": url,
                "params": params,
                "records_total": total_count,
                **context,
            }
        )

        print(f"Saved: {file_path}")
        print(f"Total records: {total_count}")

        successful_pages += 1

        if offset + LIMIT >= total_count:
            print("No more pages.")
            break

        offset += LIMIT
        page += 1
        time.sleep(SLEEP_SECONDS)

    return successful_pages


def fetch_countries(ctx) -> int:
    return fetch_reference_pages(ctx, "countries")


def fetch_aircraft(ctx) -> int:
    return fetch_reference_pages(ctx, "aircraft")

def fetch_airlines(ctx) -> int:
    return fetch_reference_pages(ctx, "airlines")


def fetch_airports(ctx) -> int:
    return fetch_reference_pages(ctx, "airports")


def fetch_cities(ctx) -> int:
    return fetch_reference_pages(ctx, "cities")

def run_reference_data_ingestion_small(ctx) -> int:
    """
    Smaller monthly reference datasets together.
    """
    total_pages = 0
    total_pages += fetch_countries(ctx)
    total_pages += fetch_aircraft(ctx)
    total_pages += fetch_airlines(ctx)
    total_pages += fetch_airports(ctx)

    print(f"Total reference pages fetched (small): {total_pages}")
    return total_pages

def run_reference_data_ingestion_cities(ctx) -> int:
    """
    Cities kept separate because of larger request volume.
    """
    total_pages = fetch_cities(ctx)
    print(f"Total reference pages fetched (cities): {total_pages}")
    return total_pages


def run_reference_data_ingestion_all(ctx) -> int:
    total_pages = 0
    total_pages += run_reference_data_ingestion_small(ctx)
    total_pages += run_reference_data_ingestion_cities(ctx)

    print(f"Total reference pages fetched (all): {total_pages}")
    return total_pages

def init_reference_data_ingestion(spark) -> str:
    """
    Ensure required log table exists and return run_id for this execution.
    """
    create_reference_data_log_table(spark)
    run_id = utc_now_str()

    return run_id