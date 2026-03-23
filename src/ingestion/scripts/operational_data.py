"""
Handles ingestion of operational data from Lufthansa API
and stores raw JSON into Unity Catalog volumes.
"""
import json
import time
from functools import partial

from config.settings import (
    BASE_URL,
    AIRPORTS,
    SLEEP_SECONDS,
)

from support.api_client import request_with_retry
from support.utils import utc_now_str, get_target_window_start
from support.paths import flight_status_directory
from support.storage import mkdirs, write_json
from support.logging import (
    append_flight_status_log,
    append_schedules_log,
)

LIMIT = 50

def fetch_airport_window(
    ctx,
    airport: str,
    target_date: str,
    window_start: str,
    direction: str,
    schedules: bool = False,
) -> int:
    """
    Fetch all paginated flight-status pages for one airport, one direction,
    and one time window. Returns the number of successfully saved pages.
    """
    append_log = append_schedules_log if schedules else append_flight_status_log
    log_func = partial(append_log, ctx.spark, ctx.run_id)

    offset = 0
    page = 0
    successful_pages = 0

    while True:
        url = f"{BASE_URL}/v1/operations/flightstatus/{direction}/{airport}/{window_start}"
        params = {
            "limit": LIMIT,
            "offset": offset,
        }

        directory = flight_status_directory(
            direction=direction,
            airport=airport,
            flight_date=target_date,
            window_start=window_start,
            run_id=ctx.run_id,
            schedules=schedules,
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

        print(
            f"[{direction}] airport={airport} "
            f"window={window_start} page={page} offset={offset}"
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
            data.get("FlightStatusResource", {})
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
        print(f"Total flights: {total_count}")

        successful_pages += 1

        if offset + LIMIT >= total_count:
            print("No more pages.")
            break

        offset += LIMIT
        page += 1
        time.sleep(SLEEP_SECONDS)

    return successful_pages


def fetch_all_airports_for_window(
    ctx,
    direction: str,
    delay_hours: int,
    schedules: bool = False,
) -> int:
    """
    Fetch one flight-status window across all configured airports.
    Returns total successful pages fetched.
    """
    window_start = get_target_window_start(delay_hours, schedules)
    if window_start.endswith("T00:00"):
        print(f"Skipping midnight window: {window_start}")
        return 0
    target_date = window_start[:10]
    total_pages = 0

    print("Starting flight-status ingestion:")
    print(f"  direction    = {direction}")
    print(f"  delay_hours  = {delay_hours}")
    print(f"  target_date  = {target_date}")
    print(f"  window_start = {window_start}")

    for airport in AIRPORTS:
        pages = fetch_airport_window(
            ctx=ctx,
            airport=airport,
            target_date=target_date,
            window_start=window_start,
            direction=direction,
            schedules=schedules
        )
        total_pages += pages
        time.sleep(SLEEP_SECONDS)

    print(f"Finished ingestion. Total pages fetched: {total_pages}")
    return total_pages