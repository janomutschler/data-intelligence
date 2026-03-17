import json
import time
from functools import partial

from config.settings import (
    BASE_URL,
    AIRPORTS,
    SLEEP_SECONDS,
)

from common.api_client import request_with_retry
from common.utils import utc_now_str, get_target_window_start
from common.paths import flight_status_directory
from common.storage import mkdirs, write_json
from common.logging import (
    create_flight_status_log_table,
    append_flight_status_log,
)

LIMIT = 50

def fetch_airport_window(
    ctx,
    airport: str,
    target_date: str,
    window_start: str,
    direction: str,
) -> int:
    """
    Fetch all paginated flight-status pages for one airport, one direction,
    and one time window. Returns the number of successfully saved pages.
    """
    log_func = partial(append_flight_status_log, ctx.spark, ctx.run_id)

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
) -> int:
    """
    Fetch one flight-status window across all configured airports.
    Returns total successful pages fetched.
    """
    window_start = get_target_window_start(delay_hours)
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
        )
        total_pages += pages
        time.sleep(SLEEP_SECONDS)

    print(f"Finished ingestion. Total pages fetched: {total_pages}")
    return total_pages

def run_flight_status_ingestion(ctx) -> int:
    
    total_pages_departures = fetch_all_airports_for_window(
        ctx=ctx,
        direction="departures",
        delay_hours=24,
    )

    total_pages_arrivals = fetch_all_airports_for_window(
        ctx=ctx,
        direction="arrivals",
        delay_hours=12,
    )

    print(f"Total pages fetched: {total_pages_departures + total_pages_arrivals}")
    return total_pages_departures + total_pages_arrivals

def init_flight_status_ingestion(spark) -> str:
    """
    Ensure required log table exists and return run_id for this execution.
    """
    create_flight_status_log_table(spark)
    run_id = utc_now_str()

    return run_id