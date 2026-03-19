import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
parser.add_argument("--volume", required=True)
args = parser.parse_args()

CATALOG = args.catalog
SCHEMA = args.schema
VOLUME = args.volume

BASE_URL = "https://api.lufthansa.com"

AIRPORTS = ["FRA", "MUC", "ZRH", "VIE", "BRU", "BER", "HAM", "DUS"]

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

REQUEST_TIMEOUT = 30
MAX_RETRIES = 10
BASE_BACKOFF = 2
SLEEP_SECONDS = 1
