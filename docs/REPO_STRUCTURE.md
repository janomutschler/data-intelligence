# Repository structure

## Root
- `databricks.yml`
  - Databricks Declarative Automation Bundle definition: targets, variables, schemas, and the managed raw landing volume.

- `resources/`
  - `resources/jobs/operational.yml` – orchestration for operational ingestion + pipeline
  - `resources/jobs/reference.yml` – orchestration for reference ingestion + pipeline
  - `resources/pipelines/pipelines.yml` – pipeline definitions and library entry points

- `docs/`
  - Human-facing documentation (this folder).

## Source code: `src/`
The code is split by lifecycle stage:

### `src/ingestion/`
Spark Python tasks that call the Lufthansa API and write JSON files into Unity Catalog volumes.
- `src/ingestion/run_reference_entrypoint.py`
- `src/ingestion/run_operational_entrypoint.py`
- `src/ingestion/scripts/reference_data.py`
- `src/ingestion/scripts/operational_data.py`
- `src/ingestion/common/` (API client, logging, storage utils, path builders)
- `src/ingestion/config.py` (airports list, time window strategy, retry settings)

Why this structure?
- Keeps external I/O (API calls + file writes) separate from transformations.
- Makes it easy to run ingestion independently from the pipeline transforms.

### `src/transformation/`
Lakeflow Spark Declarative Pipelines (DP via `pyspark.pipelines`) that implement medallion transforms.
- `bronze/` – Auto Loader -> raw JSON strings in Delta
- `silver/operational/` – JSON parsing, enrichment, DQ, CDC to SCD2
- `silver/reference/` – snapshot parsing, DQ, snapshot CDC to SCD1
- `gold/` – analytic aggregates

Why this structure?
- Mirrors the medallion architecture so reviewers can navigate quickly.
- Encapsulates Silver business logic and Gold semantic models separately.
