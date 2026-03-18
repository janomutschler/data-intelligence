from config.settings import CATALOG, SCHEMA, VOLUME

RAW_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

def flight_status_directory(
    direction: str,
    airport: str,
    flight_date: str,
    window_start: str,
) -> str:
    window_clean = window_start.replace(":", "-")

    return (
        f"{RAW_BASE_PATH}/flight_status/{direction}"
        f"/airport={airport}"
        f"/date={flight_date}"
        f"/window_start={window_clean}"
    )

def reference_data_directory(
    reference_type: str,
    reference_date: str,
) -> str:
    return (
        f"{RAW_BASE_PATH}/reference_data/{reference_type}"
        f"/date={reference_date}"
    )