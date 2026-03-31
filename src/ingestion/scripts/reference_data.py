"""
Handles ingestion of reference data from Lufthansa API
and stores raw JSON into Unity Catalog volumes.
"""
import json
import time
from functools import partial

from config import (
    BASE_URL,
    SLEEP_SECONDS,
    REFERENCE_CONFIG,
)

from common.api_client import request_with_retry
from common.utils import utc_now_str
from common.paths import reference_data_directory
from common.storage import mkdirs, write_json
from common.logging import (
    create_reference_data_log_table,
    append_reference_data_log,
)

REFERENCE_TYPES = list(REFERENCE_CONFIG.keys())

LIMIT = 100


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

    directory = reference_data_directory(
        reference_type=reference_type,
        reference_date=reference_date,
        run_id=ctx.run_id,
    )
    mkdirs(directory, ctx.dbutils)

    while True:
        url = f"{BASE_URL}/v1/references/{reference_type}"
        params = {
            "limit": LIMIT,
            "offset": offset,
            **reference_type_config["params"],
        }

        file_path = f"{directory}/page={page}.json"

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


def run_reference_ingestion(ctx) -> int:
    """
    Run ingestion for all reference datasets.

    Returns the total number of successfully saved pages.
    """
    total_pages = 0

    for reference_type in REFERENCE_TYPES:
        print(f"Starting ingestion for: {reference_type}")
        total_pages += fetch_reference_pages(ctx, reference_type)

    print(f"Total reference pages fetched: {total_pages}")
    return total_pages

def init_reference_ingestion(spark) -> str:
    """
    Ensure required log table exists and return run_id for this execution.
    """
    create_reference_data_log_table(spark)
    run_id = utc_now_str()

    return run_id