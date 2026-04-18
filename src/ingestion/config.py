SCHEMA = "raw_data"
VOLUME = "raw_lh_data"

BASE_URL = "https://api.lufthansa.com"

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# Statuses that mean "no data exists for this request (resourcenotfound)" — expected empty results, not failures.
NO_DATA_STATUSES = {404}

REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
BASE_BACKOFF = 2
SLEEP_SECONDS = 1



REFERENCE_CONFIG = {
    "countries": {
        "resource_name": "CountryResource",
        "params": {
            "lang": "en",
        },
    },
    "aircraft": {
        "resource_name": "AircraftResource",
        "params": {},
    },
    "airlines": {
        "resource_name": "AirlineResource",
        "params": {
            "lang": "en",
        },
    },
    "airports": {
        "resource_name": "AirportResource",
        "params": {
            "lang": "en",
            "LHoperated": "1",
        },
    },
    "cities": {
        "resource_name": "CityResource",
        "params": {
            "lang": "en",
        },
    },
}

AIRPORTS = ["FRA", "MUC", "ZRH", "VIE", "BRU",
            "BER", "HAM", "DUS", "LHR", "FCO"]

DEPARTURE_WINDOW_OFFSET_HOURS = [24, 20, 16, 12, 8, 4, 0, -4]
ARRIVAL_WINDOW_OFFSET_HOURS = [12, 8, 4, 0, -4, -8, -12, -16]

# Operational ingestion strategy
#
# We ingest flight-status data using fixed 4-hour windows across a selected
# set of high-traffic European airports to maximize coverage while staying
# within API rate limits.
#
# Each value in `DEPARTURE_WINDOW_OFFSET_HOURS` and `ARRIVAL_WINDOW_OFFSET_HOURS` represents
# the start of a 4-hour window relative to the current time (floored to the
# nearest 4-hour boundary):
#   - Positive values → past windows (e.g. 24 = 24–20 hours ago)
#   - 0               → current window
#   - Negative values → future windows (e.g. -4 = next 4-hour window)
#
# Coverage:
#   - Departures: from 24 hours in the past up to 8 hours in the future
#   - Arrivals:   from 12 hours in the past up to 20 hours in the future
#
# This overlapping window strategy ensures:
#   - capture of delayed and updated flight states
#   - inclusion of near-future scheduled operations
#   - resilience against missed runs or API inconsistencies
#
# Note: Duplicate records across windows are expected and handled downstream
# in the Silver layer via deduplication / upsert logic.

