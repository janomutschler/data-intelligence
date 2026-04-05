from config import SCHEMA, VOLUME


def _raw_base_path(catalog: str) -> str:
    return f"/Volumes/{catalog}/{SCHEMA}/{VOLUME}"


def flight_status_directory(
    catalog: str,
    direction: str,
    airport: str,
    flight_date: str,
    window_start: str,
    run_id: str,
    schedules: bool = False,
) -> str:
    window_clean = window_start.replace(":", "-")
    run_id_clean = run_id.replace(":", "-")
    dataset = "schedules" if schedules else "flight_status"

    return (
        f"{_raw_base_path(catalog)}/{dataset}/direction={direction}"
        f"/airport={airport}"
        f"/date={flight_date}"
        f"/window_start={window_clean}"
        f"/run_id={run_id_clean}"
    )


def reference_data_directory(
    catalog: str,
    reference_type: str,
    reference_date: str,
    run_id: str,
) -> str:
    run_id_clean = run_id.replace(":", "-")

    return (
        f"{_raw_base_path(catalog)}/reference_data/{reference_type}"
        f"/date={reference_date}"
        f"/run_id={run_id_clean}"
    )