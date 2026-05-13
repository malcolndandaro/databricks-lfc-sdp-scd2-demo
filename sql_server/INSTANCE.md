# SQL Server source — connection notes

The demo expects a SQL Server 2019+ instance reachable from your Databricks workspace, hosting a `DemoDB` database with a `crm` schema. Used by:

- DDL in `sql_server/ddl/` (run via the `sqlserver_setup` Job)
- Lakeflow Connect ingestion gateway (reads `crm.consultoras` via Change Tracking)
- Manual smoke tests (live `UPDATE` of `consultora_id = 42`'s tier during the demo)

> **This is a throwaway environment.** Provision a dedicated SQL Server VM for the demo and tear it down after. Do not reuse the SA credentials elsewhere.

## Required connection details

Set these via Databricks workspace secrets (scope `directsales-demo`, see `sql_server/ddl/README.md`) and bundle variables (`databricks.yml`):

| Field | Bundle variable / Secret | Notes |
|---|---|---|
| Host / IP | `sqlserver_host` | Reachable from the workspace VPC / public IP |
| Port | `sqlserver_port` | Default `1433` |
| Database | `sqlserver_database` | Default `DemoDB` |
| SA username | secret `sqlserver-sa-user` | Typically `SA` |
| SA password | secret `sqlserver-sa-password` | Set once via `databricks secrets put-secret …` |

### JDBC URL shape

```
jdbc:sqlserver://<host>:<port>;database=<db>;encrypt=true;trustServerCertificate=true
```

`trustServerCertificate=true` is acceptable for a throwaway demo; for production, install a CA-signed certificate on the SQL Server VM and drop the flag.

## Change Tracking, not CDC

The DDL enables **Change Tracking** (not full CDC) on `crm.consultoras`. Lakeflow Connect supports both, but Change Tracking is lighter-weight and sufficient here:

- **CDC** captures full row history (before/after images) into shadow tables — richer payload, heavier cost.
- **Change Tracking** records *which* rows changed (PK + change type) and joins back to the live table for current state.

The demo's downstream SCD2 history is captured by **AutoCDC** in the gold layer, so the source side only needs to identify *which* rows changed, not the full history. Hence Change Tracking is enough.

DDL highlights:

```sql
ALTER DATABASE DemoDB SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
ALTER TABLE crm.consultoras ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
```

The Lakeflow Connect ingestion-gateway pipeline (`resources/pipelines/lfc_consultoras.yml`) is already configured for Change Tracking mode.

## Generating live changes (for the SCD2 beat)

The "live" demo moment is a single `UPDATE` against `crm.consultoras` that flips a tier:

```sql
USE DemoDB;
UPDATE crm.consultoras
SET tier = 'ouro', updated_at = SYSUTCDATETIME()
WHERE consultora_id = 42;
```

Run this from `sqlcmd`, DBeaver, or Azure Data Studio while the audience watches `gold.dim_consultora` materialize the SCD2 change in Databricks.

## Cross-references

- Hero `consultora_id = 42` and tier-transition date are owned by `src/seed/constants.py`.
- LFC ingestion target: `${target_catalog}.bronze.consultoras_raw` (declared in `resources/pipelines/lfc_consultoras.yml`).
