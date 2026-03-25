# Data Quality Rules

This document defines the data quality rules applied across the medallion architecture of the project.
Validation is performed during transformation from Bronze to Silver. Records that fail validation are flagged as invalid and written to quarantine tables for further inspection.

## 1. Operational Data (Flight Status)

### Validation approach
Operational flight status records are validated after parsing, flattening, and standardization.
Field-level validation flags are created for critical business attributes and combined into a final `is_invalid` indicator.

### Mandatory field checks
A flight status record is marked as invalid if any of the following fields is null:

- `operating_airline_id`
- `operating_flight_number`
- `departure_airport_code`
- `arrival_airport_code`
- `scheduled_departure_utc_ts`
- `flight_date`

### Validation metadata
The following boolean flags are generated to support traceability of validation failures:

- `missing_operating_airline_id`
- `missing_operating_flight_number`
- `missing_departure_airport_code`
- `missing_arrival_airport_code`
- `missing_scheduled_departure_utc_ts`
- `missing_flight_date`

### Invalid record handling
If any mandatory field check fails, the record is marked with `is_invalid = true` and written to the quarantine table.

---

## 2. Reference Data

Reference datasets are validated after normalization and staging.
For reference data, a record is considered invalid when required identifier or name fields are null or empty strings.

### 2.1 Aircrafts
A record is marked as invalid if any of the following fields is null or empty:

- `aircraft_code`
- `aircraft_name`

### 2.2 Airlines
A record is marked as invalid if any of the following fields is null or empty:

- `airline_id`
- `airline_name`

### 2.3 Airports
A record is marked as invalid if any of the following fields is null or empty:

- `airport_code`
- `airport_name`
- `city_code`
- `country_code`

### 2.4 Cities
A record is marked as invalid if any of the following fields is null or empty:

- `city_code`
- `country_code`
- `city_name`

### 2.5 Countries
A record is marked as invalid if any of the following fields is null or empty:

- `country_code`
- `country_name`

---

## 3. Quarantine Handling

Records that fail validation are not loaded into the curated Silver tables.
Instead, they are written to dedicated quarantine tables.

Quarantine records are stored to:

- preserve invalid source records for later inspection
- support debugging of upstream data issues
- avoid silently dropping bad data
- make validation outcomes transparent during pipeline execution

---

## 4. Lineage and Traceability

To support lineage tracking from source to curated layers, Bronze ingestion stores technical metadata columns such as:

- source file path
- source file name
- source file size
- source file modification time
- ingestion timestamp

These metadata fields allow records to be traced back to their raw source files and ingestion events.

---

## 5. Schema Evolution Handling

Raw data is ingested in Bronze as unstructured JSON strings to ensure robustness against schema changes.

Schema enforcement is applied in the Silver layer using explicitly defined schemas for each dataset.

When source data does not match the expected schema:
- its marked as invalid
- send to quarantine tables

This approach ensures that schema changes do not break ingestion while still maintaining data quality in curated layers.