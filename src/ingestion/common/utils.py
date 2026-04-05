import logging
from datetime import UTC, datetime, timedelta


def utc_now_str() -> str:
    """
    Return the current UTC timestamp as an ISO-like string.
    Example: 2026-03-16T14:30:00Z
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def floor_to_4h_window(dt: datetime) -> datetime:
    """
    Round a datetime down to the latest valid 4-hour window start.
    Example:
        2026-03-14T09:15 -> 2026-03-14T08:00
    """
    window_hour = (dt.hour // 4) * 4
    return dt.replace(hour=window_hour, minute=0, second=0, microsecond=0)


def get_target_window_start(delay_hours: int = 0, schedules: bool = False) -> str:
    """
    Determine the final target window start for ingestion.

    Steps:
    1. Take the current UTC timestamp.
    2. Round it down to the latest valid 4-hour window.
    3. Apply the delay offset (subtract for historical, add for scheduled).
    4. Return as YYYY-MM-DDTHH:MM.

    Example:
        run_time = 2026-03-14T09:15 UTC
        delay_hours = 12
        -> 2026-03-13T20:00
    """
    run_time = datetime.now(UTC)
    window_start = floor_to_4h_window(run_time)

    if schedules:
        target_window = window_start + timedelta(hours=delay_hours)
    else:
        target_window = window_start - timedelta(hours=delay_hours)

    return target_window.strftime("%Y-%m-%dT%H:%M")


def configure_logging() -> None:
    """Configure root logging for Databricks job output."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
