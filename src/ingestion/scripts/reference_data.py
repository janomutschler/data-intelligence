"""
Handles ingestion of reference data from Lufthansa API
and stores raw JSON into Unity Catalog volumes.
"""
import json
import logging
import time
from functools import partial
from typing import Callable

from common.api_client import request_with_retry
from common.context import IngestionContext
from common.logging import append_reference_data_log, create_reference_data_log_table
from common.paths import reference_data_directory
from common.storage import mkdirs, write_json
from common.utils import get_target_window_start, utc_now_str
from config import (
    BASE_URL,
    REFERENCE_CONFIG,
    SLEEP_SECONDS,
)

logger = logging.getLogger(__name__)

REFERENCE_TYPES = list(REFERENCE_CONFIG.keys())
LIMIT = 100


def fetch_reference_pages(ctx: IngestionContext, reference_type: str) -> int:
    """
    Fetch all paginated pages for one reference dataset.
    Returns the number of successfully saved pages.
    """
    log_func: Callable = partial(
        append_reference_data_log, ctx.spark, ctx.catalog, ctx.run_id
    )

    offset = 0
    page = 0
    successful_pages = 0
    reference_date = utc_now_str()[:10]

    reference_type_config = REFERENCE_CONFIG[reference_type]
    resource_name = reference_type_config["resource_name"]

    directory = reference_data_directory(
        catalog=ctx.catalog,
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

        logger.info("[reference] type=%s page=%d offset=%d", reference_type, page, offset)

        response = request_with_retry(
            session=ctx.session,
            url=url,
            params=params,
            context=context,
            log_func=log_func,
        )

        if response is None:
            # Either a real failure (logged as 'failed') or no data for this offset
            # (logged as 'no_data'). Either way, stop pagination cleanly.
            break

        try:
            data = response.json()
        except ValueError:
            logger.error("Invalid JSON in response — stopping pagination.")
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
            logger.warning("Missing TotalCount in response — stopping pagination.")
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

        logger.info("Saved %s — total records: %d", file_path, total_count)
        successful_pages += 1

        if offset + LIMIT >= total_count:
            logger.info("All pages fetched for %s.", reference_type)
            break

        offset += LIMIT
        page += 1
        time.sleep(SLEEP_SECONDS)

    return successful_pages


def run_reference_ingestion(ctx: IngestionContext) -> int:
    """
    Run ingestion for all reference datasets.
    Returns the total number of successfully saved pages.
    """
    total_pages = sum(
        fetch_reference_pages(ctx, reference_type)
        for reference_type in REFERENCE_TYPES
    )
    logger.info("Reference ingestion complete. Total pages fetched: %d", total_pages)
    return total_pages


def init_reference_ingestion(spark, catalog: str) -> str:
    """
    Ensure required log table exists and return the run_id for this execution.
    """
    create_reference_data_log_table(spark, catalog)
    return utc_now_str()

