# Data quality

## Quality philosophy
This pipeline makes data quality behaviour explicit:

- **Quarantine tables** preserve invalid records for investigation.
- **Expectations** enforce constraints on curated outputs and record drop metrics.

In practice:
- Invalid rows are written to quarantine.
- Invalid rows are dropped from curated source feeds and Gold outputs using `expect_all_or_drop`.

## Operational (flight status) rules

### Mandatory fields (Silver)
A flight status record is invalid if any of the following are missing:
- `operating_airline_id`
- `operating_flight_number`
- `departure_airport_code`
- `arrival_airport_code`
- `scheduled_departure_utc_ts`
- `flight_date`

Actions:
- Sent to: `silver.flight_status_quarantine` with a `quarantine_reason`
- Dropped from: `silver.flight_status_cdc_source` via `expect_all_or_drop`
- Therefore excluded from: `silver.flight_status_history` / `silver.flight_status_current`

### Timestamp parsing expectations
- UTC timestamps must parse into Spark timestamps (`scheduled_departure_utc_ts IS NOT NULL` is enforced).
- Local timestamps are parsed when present; null is tolerated.

### Key integrity (CDC)
- The CDC key is `flight_instance_hash_key`.
- The CDC sequence column is `sequence_ts` derived from source file modification time.
- CDC stored as SCD2 in `silver.flight_status_history`.

## Reference data rules (per entity)

All reference entities follow the same pattern:
- Stage and clean values (trim/upper).
- Quarantine invalid rows.
- Produce a valid snapshot (deduplicated by business keys).
- Apply snapshot-based Auto CDC into a SCD1 “current” table.

### Aircraft
Invalid if:
- `aircraft_code` is null/empty
- `aircraft_name` is null/empty

Actions:
- Invalid -> `silver.aircraft_quarantine`
- Valid snapshot -> `silver.aircraft_snapshot_valid` (private)
- Current table -> `silver.aircraft_current` (SCD1)

### Airlines
Invalid if:
- `airline_id` is null/empty
- `airline_name` is null/empty

### Airports
Invalid if:
- `airport_code` is null/empty
- `airport_name` is null/empty
- `city_code` is null/empty
- `country_code` is null/empty

### Cities
Invalid if:
- `city_code` is null/empty
- `country_code` is null/empty
- `city_name` is null/empty

### Countries
Invalid if:
- `country_code` is null/empty
- `country_name` is null/empty

## Gold layer rules

Gold tables are protected by expectations to prevent nonsensical KPI values.

Examples:
- Percentage KPIs must be between 0 and 1 (OTP rates).
- Counts cannot be negative.
- Hour is constrained to 0–23.

If Gold expectations fail:
- rows are dropped (and metrics are logged), preserving pipeline continuity.

## How to inspect quality outcomes (SQL snippets)
Use your real catalog name (e.g. `data_intelligence_dev`).

```sql
-- Quarantine volume (operational)
SELECT quarantine_reason, COUNT(*) AS rows
FROM <catalog>.silver.flight_status_quarantine
GROUP BY quarantine_reason
ORDER BY rows DESC;

-- Reference quarantine (example: airports)
SELECT validation_error, COUNT(*) AS rows
FROM <catalog>.silver.airports_quarantine
GROUP BY validation_error
ORDER BY rows DESC;
```
