import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
args = parser.parse_args()

CATALOG = args.catalog
SCHEMA = "raw_data"
VOLUME = "raw_lh_data"

BASE_URL = "https://api.lufthansa.com"

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

REQUEST_TIMEOUT = 30
MAX_RETRIES = 10
BASE_BACKOFF = 2
SLEEP_SECONDS = 1


AIRPORTS = ["FRA", "MUC", "ZRH", "VIE", "BRU",
            "BER", "HAM", "DUS", "LHR", "FCO"]
        
"""
This list of airports is designed to maximize:
  1. Flight volume (high-traffic hubs → more data)
  2. Network diversity (Europe + intercontinental routes)
  3. Lufthansa relevance (LH Group + major partner hubs)

The selection follows a structured approach:

1. Core LH / DACH hubs (existing)
   - FRA, MUC → primary Lufthansa hubs (highest traffic)
   - BER, HAM, DUS → major German airports
   - VIE, BRU, ZRH → LH Group hubs (Austrian, Brussels, Swiss)

2. Major European hubs (high connectivity, strong traffic)
   - CDG (Paris), LHR (London), AMS (Amsterdam)
   - MAD (Madrid), FCO (Rome), BCN (Barcelona)

3. Secondary European airports (regional diversity)
   - MXP (Milan), VCE (Venice)
   - CPH (Copenhagen), ARN (Stockholm)
   - DUB (Dublin), LIS (Lisbon)

4. Intercontinental hubs (long-haul + analytics value)
   - USA: JFK (New York), EWR (Newark), ORD (Chicago), LAX (Los Angeles)
   - Middle East: DXB (Dubai), DOH (Doha)
   - Asia: SIN (Singapore), DEL (Delhi)

Why this matters:
- Increases number of flights ingested per hour significantly
- Enables richer analysis in later stages (e.g. delays by region,
  hub vs regional performance, long-haul vs short-haul)
- Keeps API usage efficient (~60–70 calls/hour << 1000 limit)

This setup provides a strong foundation for building a realistic
aviation data intelligence pipeline.
"""

