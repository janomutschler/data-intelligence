# Operations and deployment

## Deployment model
This project is deployed as a Databricks Declarative Automation Bundle (DAB).
Bundle deployments create/update:
- Catalog + schemas + volume definitions (Unity Catalog)
- Jobs (Lakeflow Jobs / Workflows)
- Pipelines (Lakeflow Spark Declarative Pipelines)
- Lakeview dashboard definition and its SQL warehouse binding

## Environments
Targets are configured in `databricks.yml`:
- `dev` (default, development mode)
- `prod` (production mode, with root_path set)

Catalog naming convention (catalog has to be created manually before deployment):
- `data_intelligence_${bundle.target}`

The bundle also requires a SQL warehouse id for dashboard queries. Set it as an environment-backed bundle variable before validating or deploying:

```bash
export BUNDLE_VAR_warehouse_id=<their-id>
```

This value is consumed by `resources/dashboard.yml` through `${var.warehouse_id}`.

## Deploy + run (CLI)

### Validate
```bash
export BUNDLE_VAR_warehouse_id=<their-id>
databricks bundle validate -t dev
```

### Deploy
```bash
export BUNDLE_VAR_warehouse_id=<their-id>
databricks bundle deploy -t dev
```

### Run jobs manually (recommended for demos)
Run reference first:
```bash
databricks bundle run reference_end_to_end -t dev
```

Then run operational:
```bash
databricks bundle run operational_end_to_end -t dev
```

Or run the bootstrap job, which runs reference first and operational second:

```bash
databricks bundle run system_bootstrap -t dev
```

## Scheduling
Jobs include a `schedule` block with:
- `quartz_cron_expression`
- `timezone_id` (UTC in repo)
- `pause_status` (`PAUSED` for the base job definitions)

To enable scheduling for production:
- deploy the `prod` target, which overrides the reference and operational jobs to `UNPAUSED`
  OR
- unpause directly in the Databricks UI

## Secrets and security
Required secrets:
- scope: `lh-api`
- keys: `client_id`, `client_secret`

Security practices:
- store API credentials only in secret scopes
- restrict Unity Catalog privileges (volume + schemas) to least privilege
- use UC-enabled compute for volume access (`/Volumes/...`)

## Observability

### Ingestion logs (Delta tables)
- `<catalog>.raw_data.ingestion_log_flight_status`
- `<catalog>.raw_data.ingestion_log_reference_data`

What they record:
- timestamp, run_id, status, http_status, url, params, file_path, record counts

### Pipeline health
Recommended checks:
- Most recent Job run status (Jobs UI)
- Pipeline update result + expectations metrics (Pipelines UI)
- Quarantine row counts (SQL)

## Troubleshooting runbook

### 401/403 from Lufthansa API
Symptoms:
- ingestion logs show failed requests
Likely causes:
- missing/incorrect secrets in `lh-api` scope
- invalid credentials
Actions:
- verify secret scope and keys
- manually request a token (outside pipeline) to confirm credentials

### 429 rate limiting
Symptoms:
- logs show many retries; slow runtime
Actions:
- reduce airport list or windows temporarily for demo
- keep schedules PAUSED and run on demand
- consider increasing sleep/backoff if needed

### Gold pipeline failure due to missing reference tables
Symptoms:
- distance category Gold table fails (missing `silver.airports_current`)
Action:
- run `reference_end_to_end` first, or use `system_bootstrap` for a first-time environment
