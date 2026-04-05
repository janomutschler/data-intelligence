"""
Handles ingestion of operational flight-status data from the Lufthansa API
and stores raw JSON files in Unity Catalog volumes.
"""
import json
import logging
import time
from functools import partial
from typing import Callable

from common.api_client import request_with_retry
from common.context import IngestionContext
from common.logging import append_flight_status_log, create_flight_status_log_table
from common.paths import flight_status_directory
from common.storage import mkdirs, write_json
from common.utils import get_target_window_start, utc_now_str
from config import (
    AIRPORTS,
    ARRIVAL_WINDOW_OFFSET_HOURS,
    BASE_URL,
    DEPARTURE_WINDOW_OFFSET_HOURS,
    SLEEP_SECONDS,
)

logger = logging.getLogger(__name__)

LIMIT = 50


def fetch_airport_window(
    ctx: IngestionContext,
    airport: str,
    target_date: str,
    window_start: str,
    direction: str,
) -> int:
    """
    Fetch all paginated flight-status pages for one airport, one direction,
    and one 4-hour window.

    Returns the number of successfully saved pages.
    """
    log_func: Callable = partial(
        append_flight_status_log, ctx.spark, ctx.catalog, ctx.run_id
    )

    offset = 0
    page = 0
    successful_pages = 0

    while True:
        url = f"{BASE_URL}/v1/operations/flightstatus/{direction}/{airport}/{window_start}"
        params = {"limit": LIMIT, "offset": offset}

        directory = flight_status_directory(
            catalog=ctx.catalog,
            direction=direction,
            airport=airport,
            flight_date=target_date,
            window_start=window_start,
            run_id=ctx.run_id,
        )
        file_path = f"{directory}/page={page}.json"
        mkdirs(directory, ctx.dbutils)

        context = {
            "timestamp_utc": utc_now_str(),
            "airport": airport,
            "flight_date": target_date,
            "window_start": window_start,
            "direction": direction,
            "page": page,
            "offset": offset,
            "file_path": file_path,
        }

        logger.info(
            "[%s] airport=%s window=%s page=%d offset=%d",
            direction,
            airport,
            window_start,
            page,
            offset,
        )

        response = request_with_retry(
            session=ctx.session,
            url=url,
            params=params,
            context=context,
            log_func=log_func,
        )

        if response is None:
            # Either a real failure (logged as 'failed') or no data for this window
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
            data.get("FlightStatusResource", {})
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

        logger.info("Saved %s — total flights in window: %d", file_path, total_count)
        successful_pages += 1

        if offset + LIMIT >= total_count:
            logger.info("All pages fetched for window.")
            break

        offset += LIMIT
        page += 1
        time.sleep(SLEEP_SECONDS)

    return successful_pages


def fetch_all_airports_for_window(
    ctx: IngestionContext,
    direction: str,
    delay_hours: int,
) -> int:
    """
    Fetch one 4-hour flight-status window across all configured airports.

    Returns the total number of successfully saved pages.
    """
    window_start = get_target_window_start(delay_hours)
    target_date = window_start[:10]
    total_pages = 0

    logger.info(
        "Starting flight-status ingestion: direction=%s delay_hours=%d "
        "target_date=%s window_start=%s",
        direction,
        delay_hours,
        target_date,
        window_start,
    )

    for airport in AIRPORTS:
        total_pages += fetch_airport_window(
            ctx=ctx,
            airport=airport,
            target_date=target_date,
            window_start=window_start,
            direction=direction,
        )
        time.sleep(SLEEP_SECONDS)

    logger.info("Finished window ingestion. Pages fetched: %d", total_pages)
    return total_pages


def init_operational_ingestion(spark, catalog: str) -> str:
    """
    Ensure required log table exists and return the run_id for this execution.
    """
    create_flight_status_log_table(spark, catalog)
    return utc_now_str()


def run_flight_status_ingestion(ctx: IngestionContext) -> int:
    """
    Run flight-status ingestion for all configured departure and arrival windows.

    Returns the total number of successfully saved pages.
    """
    total_pages_departures = sum(
        fetch_all_airports_for_window(ctx=ctx, direction="departures", delay_hours=h)
        for h in DEPARTURE_WINDOW_OFFSET_HOURS
    )
    total_pages_arrivals = sum(
        fetch_all_airports_for_window(ctx=ctx, direction="arrivals", delay_hours=h)
        for h in ARRIVAL_WINDOW_OFFSET_HOURS
    )

    total_pages = total_pages_departures + total_pages_arrivals
    logger.info("Ingestion complete. Total pages fetched: %d", total_pages)
    return total_pages

