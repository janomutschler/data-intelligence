import os

BASE_URL = "https://api.lufthansa.com"

CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "data_intelligence_dev")
VOLUME = os.getenv("VOLUME", "raw_lh_data")

AIRPORTS = ["FRA", "MUC", "ZRH", "VIE", "BRU", "BER", "HAM", "DUS"]

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

REQUEST_TIMEOUT = 30
MAX_RETRIES = 10
BASE_BACKOFF = 2
SLEEP_SECONDS = 1
