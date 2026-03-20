from scripts.operational_data import fetch_all_airports_for_window
from support.utils import utc_now_str
from support.logging import create_flight_status_log_table

def run_flight_status_ingestion(ctx) -> int:
    """
    Run the flight_status ingestion.
    Ingests flights stauses for arrvials and depatures leaving or arriving at one of our
    Airports for a window starting at the current time (floored to 4) - delay_hours for 4 hours
    Returns the total number of pages fetched.
    """
    total_pages_departures = fetch_all_airports_for_window(
        ctx=ctx,
        direction="departures",
        delay_hours=24,
        schedules=False,
    )

    total_pages_arrivals = fetch_all_airports_for_window(
        ctx=ctx,
        direction="arrivals",
        delay_hours=12,
        schedules=False,
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