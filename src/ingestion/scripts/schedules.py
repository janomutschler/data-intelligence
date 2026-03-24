from support.utils import utc_now_str
from scripts.operational_data import fetch_all_airports_for_window

def run_schedules_ingestion(ctx) -> int:
    """
    Run the schedules ingestion.
    Ingests flights schedules for arrvials and depaturesleaving or arriving at one of our
    Airports for a window starting at the current time (floored to 4) + delay_hours for 4 hours 
    Returns the total number of pages fetched.
    """
    total_pages_departures = fetch_all_airports_for_window(
        ctx=ctx,
        direction="departures",
        delay_hours=4,
        schedules=True,
    )

    total_pages_arrivals = fetch_all_airports_for_window(
        ctx=ctx,
        direction="arrivals",
        delay_hours=20,
        schedules=True,
    )

    print(f"Total pages fetched: {total_pages_departures + total_pages_arrivals}")
    return total_pages_departures + total_pages_arrivals

