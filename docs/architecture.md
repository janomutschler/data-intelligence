# Pipeline Architecture Overview

## Medallion Architecture

The pipeline follows a medallion architecture:

- Bronze: raw ingestion of JSON data using Auto Loader
- Silver: parsing, normalization, validation, and enrichment
- Gold: aggregated analytical tables for reporting

## Data Flow

1. Data is ingested from external APIs into the volume raw_lh_data as JSON files
2. Using Auto Loader the files in the Volume are loaded as raw unstructured JSON strings to the Bronze table
2. Bronze data is parsed and transformed into structured Silver tables
3. Validation rules are applied; invalid records are written to quarantine
4. Clean data is aggregated into Gold tables for analytics

## Key Features

- Incremental ingestion using Auto Loader
- Schema enforcement in Silver
- Data quality validation with quarantine tables
- SCD handling for reference data
- Lineage tracking via metadata columns