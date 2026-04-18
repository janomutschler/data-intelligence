# ✈️ Lufthansa Data Intelligence Pipeline

A production-style lakehouse pipeline that ingests Lufthansa API data, lands raw JSON in Unity Catalog volumes, transforms it through a Medallion architecture (Bronze/Silver/Gold), and serves analytics-ready tables for dashboards.

![Airport Operations Dashboard](./docs/preview/preview_dashboard_section_1.png)

[Dashboard documentation](./docs/DASHBOARD.md) | [Full dashboard preview](./docs/preview/preview_dashboard_full.pdf)

## Problem statement
Operational flight performance data is high-volume, frequently updated, and often inconsistent across endpoints/runs. The goal of this project is to build an end-to-end, automated, reproducible pipeline that:
- lands raw API payloads safely (schema-flexible),
- enforces data quality and traceability in curated layers,
- maintains change history where needed (CDC/SCD),
- produces Gold aggregates that can power dashboards and KPI analysis.

## Tech stack
- Databricks Lakehouse
- Lakeflow Jobs (Workflows)
- Lakeflow Spark Declarative Pipelines (dp via `pyspark.pipelines`)
- Apache Spark
- Delta Lake (tables)
- Unity Catalog (catalog/schemas/volumes)
- Lufthansa API

## What is deployed
This repo is a Databricks Declarative Automation Bundle (DAB). The bundle defines:
- Catalog: `data_intelligence_${bundle.target}`
- Schemas: `raw_data`, `bronze`, `silver`, `gold`
- Managed UC Volume: `raw_data.raw_lh_data`
- Jobs:
  - `operational_end_to_end` (operational ingestion + medallion pipeline)
  - `reference_end_to_end` (reference ingestion + medallion pipeline)
  - `system_bootstrap` (runs reference first, then operational)
- Pipelines:
  - `operational-medallion-pipeline`
  - `reference-medallion-pipeline`
- Dashboard:
  - `airport_ops_dashboard` backed by the Gold schema

## Repository quick start

### ⚠️ Lufthansa API Access

The Lufthansa OpenAPI registration process can be unreliable.
At the time of writing their sign up form is not working.

To get access, you may need to:
1. Create an account on the [Mashery Developer Portal](https://support.mashery.com/member/register)
2. Log in with that account at [Lufthansa Developer Network](https://developer.lufthansa.com)
3. Then create an application to obtain:
   - `client_id` (Key)
   - `client_secret` (Secret)

### Prerequisites
1. Databricks workspace access (Unity Catalog enabled).
2. Target catalog exists before deployment (for example, `data_intelligence_dev`).
3. Databricks CLI installed and authenticated.
4. SQL warehouse available for Lakeview dashboard queries.
5. Secrets configured in Databricks:
   - Secret scope: `lh-api`
   - Keys:
     - `client_id`
     - `client_secret`

### Validate + deploy the bundle (dev)
From repo root:
```bash
export BUNDLE_VAR_warehouse_id=<your-sql-warehouse-id>
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

> If you are not using the `DEFAULT` cli profile, specify your profile:

```bash
databricks bundle deploy -t dev --profile <your-profile>
```

The `BUNDLE_VAR_warehouse_id` environment variable is required because the bundle injects it into `resources/dashboard.yml` as `${var.warehouse_id}` for dashboard query execution.

### Run end-to-end (manual runs recommended for demos)
Run **reference first** (creates/refreshes reference dimension tables used by Gold distance metrics):
```bash
databricks bundle run reference_end_to_end -t dev
```

Then run operational:
```bash
databricks bundle run operational_end_to_end -t dev
```

Or run the bootstrap job, which runs reference and then operational in the correct order:
```bash
databricks bundle run system_bootstrap -t dev
```

> Note: Schedules in `resources/jobs.yml` are set to `PAUSED` by default for `dev`. The `prod` target overrides the reference and operational jobs to `UNPAUSED`.

## What tables you should see

### Raw landing (files)
Unity Catalog volume:
- `/Volumes/<catalog>/raw_data/raw_lh_data/reference_data/...`
- `/Volumes/<catalog>/raw_data/raw_lh_data/flight_status/...`

### Bronze (raw JSON in Delta)
- `<catalog>.bronze.flight_status_raw`
- `<catalog>.bronze.airports_raw`
- `<catalog>.bronze.airlines_raw`
- `<catalog>.bronze.aircraft_raw`
- `<catalog>.bronze.cities_raw`
- `<catalog>.bronze.countries_raw`

### Silver (curated + validated)
Operational:
- `<catalog>.silver.flight_status_quarantine`
- `<catalog>.silver.flight_status_history` (SCD2)
- `<catalog>.silver.flight_status_current`

Reference (SCD1 current dimensions + quarantine):
- `<catalog>.silver.airports_current`, `<catalog>.silver.airports_quarantine`
- `<catalog>.silver.airlines_current`, `<catalog>.silver.airlines_quarantine`
- `<catalog>.silver.aircraft_current`, `<catalog>.silver.aircraft_quarantine`
- `<catalog>.silver.cities_current`, `<catalog>.silver.cities_quarantine`
- `<catalog>.silver.countries_current`, `<catalog>.silver.countries_quarantine`

### Gold (analytics-ready)
- `<catalog>.gold.departure_airport_hourly`
- `<catalog>.gold.route_daily_performance`
- `<catalog>.gold.airport_distance_category_daily_performance`

## Documentation
- `docs/ARCHITECTURE.md` – system architecture + medallion mapping
- `docs/DASHBOARD.md` – dashboard purpose, previews, metrics, data sources, and refresh flow
- `docs/REPO_STRUCTURE.md` – repo layout and rationale
- `docs/PIPELINE.md` – job/pipeline flow, schedules, table lineage
- `docs/DATA_QUALITY.md` – rules + actions (drop vs quarantine)
- `docs/OPERATIONS_AND_DEPLOYMENT.md` – deploy/runbook/troubleshooting
<br>

## 🧠 Implementation Notes

### Alternative: Custom PySpark Implementation

An earlier version of this project (see `custom-pyspark` branch) implements the data pipelines using pure PySpark instead of Databricks-native features such as Declarative Pipelines.

This version was built to understand the underlying mechanics of data pipelines, including:
- Manual schema handling
- Incremental processing
- Data normalization and transformations
- Custom orchestration patterns

The main branch reflects a more production-oriented approach, leveraging Databricks-native functionality for:
- Maintainability
- Scalability
- Reliability
- Observability

This project therefore demonstrates both:
- The ability to use modern platform features efficiently  
- A solid understanding of how these systems work under the hood

## Reference

This project builds upon the Level 3 [Data Intelligence Track](https://github.com/arkadiahn/LEVEL3-projects/tree/main/data-intelligence) with Lufthansa Industry Solutions
