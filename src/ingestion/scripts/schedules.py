from support.utils import utc_now_str
from support.logging import create_schedules_log_table
from scripts.operational_data import fetch_all_airports_for_window

def run_schedules_ingestion(ctx) -> int:
    """
    Run the schedules ingestion.
    Returns the total number of pages fetched.
    """
    total_pages_departures = fetch_all_airports_for_window(
        ctx=ctx,
        direction="departures",
        delay_hours=12,
        schedules=True,
    )

    total_pages_arrivals = fetch_all_airports_for_window(
        ctx=ctx,
        direction="arrivals",
        delay_hours=24,
        schedules=True,
    )

    print(f"Total pages fetched: {total_pages_departures + total_pages_arrivals}")
    return total_pages_departures + total_pages_arrivals

def init_schedules_ingestion(spark) -> str:
    """
    Ensure required log table exists and return run_id for this execution.
    """
    create_schedules_log_table(spark)
    run_id = utc_now_str()

    return run_id