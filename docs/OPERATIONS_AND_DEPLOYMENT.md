# Operations and deployment

## Deployment model
This project is deployed as a Databricks Declarative Automation Bundle (DAB).
Bundle deployments create/update:
- Catalog + schemas + volume definitions (Unity Catalog)
- Jobs (Lakeflow Jobs / Workflows)
- Pipelines (Lakeflow Spark Declarative Pipelines)

## Environments
Targets are configured in `databricks.yml`:
- `dev` (default, development mode)
- `prod` (production mode, with root_path set)

Catalog naming convention (Catalog has to be created manualy before deployment):
- `data_intelligence_${bundle.target}`

## Deploy + run (CLI)

### Validate
```bash
databricks bundle validate -t dev
```

### Deploy
```bash
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

## Scheduling
Jobs include a `schedule` block with:
- `quartz_cron_expression`
- `timezone_id` (UTC in repo)
- `pause_status` (PAUSED in repo)

To enable scheduling for production:
- update YAML to `pause_status: UNPAUSED`, redeploy the bundle
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
- run `reference_end_to_end` first
