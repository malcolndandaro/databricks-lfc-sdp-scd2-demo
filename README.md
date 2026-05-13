# Direct-Sales E2E Demo on the Databricks Lakehouse

> An end-to-end Databricks demo built around a direct-sales LATAM business — SQL Server CDC, declarative streaming pipelines, SCD2 in three lines, materialized views, and a live row-level-security flip. Everything declarative. Everything on serverless.

```
SQL Server (DemoDB)              Databricks Workspace
┌────────────────┐
│ crm.consultoras│ ── LFC (Change Tracking) ──┐
└────────────────┘                            ▼
                                  ┌───────────────────────────────────────────┐
UC Volume (parquet)               │   BRONZE         SILVER         GOLD      │
┌────────────────┐                │  ┌──────┐       ┌──────┐       ┌──────┐  │
│ pedidos/*.parq │ ── Auto ──────►│  │ raw  │ ────► │ typed│ ────► │ dim/ │  │
│  drops in lz/  │   Loader       │  │ 1:1  │       │ dedup│       │ fact │  │
└────────────────┘                │  │ CDF  │       │ 1:1  │       │ + MV │  │
                                  │  └──────┘       └──────┘       └──┬───┘  │
                                  │                                    │      │
                                  │                ┌───────────────────┘      │
                                  │                ▼                          │
                                  │   ┌────────────────────────┐              │
                                  │   │  RLS + Column Masking  │              │
                                  │   │  via gold.acl_consultora│             │
                                  │   └────────────────────────┘              │
                                  └───────────────────────────────────────────┘
                                       AutoCDC ▸ SCD2 + SCD1 ▸ MVs
```

## What this demo shows

| Capability | Where to look | Why it's interesting |
|---|---|---|
| **Change Data Capture from SQL Server** | `resources/pipelines/lfc_consultoras.yml` | Lakeflow Connect, Change Tracking mode (lighter than full CDC). Zero-glue ingestion. |
| **Auto Loader + schema evolution** | `src/sdp/00_bronze.sql` + `scripts/demo_schema_evolution.py` | New parquet columns flow through bronze without manual migration. Live beat. |
| **SDP declarative pipelines on serverless** | `src/sdp/*` | One YAML, one pipeline definition. Streaming tables, MVs, full lineage. |
| **AutoCDC SCD2 + SCD1** | `src/sdp/02_gold_dim_fact.sql` | Hand-rolled MERGE INTO collapses to 3 lines of declarative APPLY CHANGES INTO. |
| **Incremental MVs over AutoCDC** | `src/sdp/03_gold_reports.sql` | Materialized Views auto-refresh from upstream AutoCDC streaming tables. Retires the merge-with-cache pattern. |
| **Row-Level Security + Column Masking** | `src/notebooks/create_governance.py` | Control-table pattern (`acl_consultora`) drives EXISTS-based row filter + cpf mask. Live grant flip. |
| **DABs + GitHub Actions** | `databricks.yml`, `.github/workflows/` | Dev/prod gated by branch protection. PR validate runs SQL lint + secret scan. |
| **QA pain-points pipeline** | `src/qa_demos/` | Standalone SDP: type widening, ForEachBatch sink, REPLACE WHERE — three advanced patterns side-by-side. |

## The live demo beats

These are the moments where you point at the screen and say *"watch this"*:

**1. SCD2 in seconds.** A single `UPDATE crm.consultoras SET tier = 'ouro' WHERE consultora_id = 42` on SQL Server flows through LFC → bronze → silver → and lands as a new SCD2 row in `gold.dim_consultora` within a minute. No glue, no hand-rolled MERGE, no orchestration code.

**2. Schema evolution on the fly.** Drop a parquet file with a new column (`canal_origem`) into the bronze landing zone. The next streaming-table update widens the schema through bronze and silver automatically — `addNewColumns` mode does the work.

**3. RLS in one INSERT.** Before: every CPF masked, every region invisible. INSERT one row into `gold.acl_consultora`, re-run the query: rows from the granted region appear, CPF unmasks. No redeploy. No group sync.

**4. Materialized View consistency.** Historic-tier commissions: an MV joins `gold.dim_consultora` (SCD2) with `gold.fact_pedido` (SCD1) using the *as-of-pedido-date* tier. Re-runs incrementally as new pedidos land — no full reprocessing.

## Quick start

```bash
# 1. Configure
cp databricks.yml databricks.yml.bak     # back up the template
# Edit databricks.yml: set workspace.host, sqlserver_host,
# notification_email, catalog_owner, and (for prod) root_path user.

# 2. Authenticate + install dev deps
databricks auth login --host https://<your-workspace>.cloud.databricks.com
pip install -r requirements-dev.txt

# 3. Store SQL Server credentials
databricks secrets create-scope directsales-demo
databricks secrets put-secret directsales-demo sqlserver-sa-user --string-value "SA"
databricks secrets put-secret directsales-demo sqlserver-sa-password
# (interactive prompt — paste the SA password)

# 4. Deploy + seed + run
databricks bundle deploy -t dev
databricks bundle run sqlserver_setup -t dev          # creates UC connection, seeds SQL Server, full-refreshes the SDP pipeline
python -m src.seed.generate_pedidos_parquet \
    --upload-to /Volumes/directsales_dev/bronze/lz/pedidos
databricks bundle run sdp_main -t dev                 # builds bronze → silver → gold
databricks bundle run governance_setup -t dev         # creates acl_consultora, RLS, column mask
```

That's it — the data flows once you trigger the SDP pipeline. From here, the **live demo beats** above are the interactive bits.

## Project layout

```
.
├── databricks.yml              # Bundle definition (dev + prod targets)
├── resources/                  # Declarative bundle resources
│   ├── catalogs/               #   UC catalog + schemas
│   ├── connections/            #   UC connection setup notes (workspace-level, see README)
│   ├── volumes/                #   UC volume for Auto Loader landing zone
│   ├── pipelines/              #   SDP pipelines: main (bronze→silver→gold), LFC, QA demos
│   └── jobs/                   #   Workflow Jobs: SQL Server setup, governance, gold UDFs
├── src/
│   ├── sdp/                    # SDP pipeline code: bronze, silver, gold (dim/fact + MVs)
│   ├── notebooks/              # Job-task notebooks: governance, UC connection, UDFs
│   ├── seed/                   # Synthetic data generators (Consultoras SQL + Pedidos parquet)
│   ├── qa_demos/               # Standalone SDP pipeline for advanced patterns
│   └── tests/                  # pytest integration tests (databricks-connect serverless)
├── sql_server/
│   ├── INSTANCE.md             # SQL Server connection notes (placeholders — set your own)
│   └── ddl/                    # Idempotent T-SQL DDL: schema, table, Change Tracking,
│                               #   seed data, backdated tier-transition updates
├── scripts/                    # Local helpers (demo reset + schema-evolution beat)
├── .github/workflows/          # PR validate · deploy-dev · deploy-prod
├── Makefile                    # `make reset` shortcuts (dev only)
└── requirements-dev.txt        # pytest, faker, databricks-connect, databricks-sdk
```

## Domain glossary

The data model uses direct-sales LATAM terminology:

| Term | Meaning |
|---|---|
| **Consultora / Consultor** | Direct-sales representative (feminine / masculine forms) |
| **Pedido** | Order placed through a Consultora |
| **Tier** | Consultora's program ranking: `semente` → `bronze` → `prata` → `ouro` → `diamante` |
| **Comissão** | Commission on a Pedido, computed from the Consultora's tier *at the time of the Pedido* (hence SCD2 on `dim_consultora`) |

Layer convention:

- **Bronze** — raw landing zone, no cleaning
- **Silver** — curated 1:1 per source: type-cast, deduped, renamed. **No** SCD typing.
- **Gold** — Kimball-style dim/fact (AutoCDC SCD2 + SCD1) + Materialized View reports

## Design highlights

A few non-obvious decisions worth calling out:

**AutoCDC as the lead pillar.** A typical hand-rolled CDC pipeline is a forest of MERGE INTO statements with caching, retry logic, and bespoke watermarks. Here, `gold.dim_consultora` and `gold.fact_pedido` are 8-line `APPLY CHANGES INTO` declarations — SCD2 and SCD1 native, Change Data Feed preserved automatically, downstream MVs read incrementally.

**Materialized Views over streaming tables.** The historic-tier commission report (`mv_comissao_historic`) is an MV on top of two AutoCDC streaming tables. SDP figures out the incremental refresh — no rollup-table-with-cache pattern needed.

**`acl_consultora` instead of `is_account_group_member`.** The standard RLS pattern in Databricks uses IdP groups. This demo's environment doesn't allow on-the-fly group mutation, so RLS + masking go through a 3-column control table (`email`, `regiao`, `cpf`). The live grant flip is `INSERT INTO gold.acl_consultora …`. In a customer environment with IdP integration, swap the `EXISTS(acl_consultora …)` body for `is_account_group_member('group')` — same shape, IdP-native.

**Bronze stores current state, CDF stores history.** `bronze.consultoras_raw` is MERGE-driven (500 rows always). Silver re-projects bronze's CDF as an event stream, which AutoCDC consumes to build the SCD2 history in gold. This is a deliberate trade-off: bronze is queryable as "current source state" without window-walking; full row history lives in CDF + gold.

**Change Tracking, not full CDC.** SQL Server CDC captures full before/after row images into shadow tables — heavy. Change Tracking just records *which* rows changed; LFC joins back to the live table for current state. AutoCDC builds the SCD2 history downstream, so the source side only needs to identify the deltas. Lighter, faster, cheaper.

## QA-pain-points pipeline

`src/qa_demos/` is a standalone SDP pipeline that demonstrates three advanced patterns the main pipeline doesn't cover live. Useful as a Q&A backup when the conversation goes deep:

| File | Pattern | Status |
|---|---|---|
| `01_type_widening.sql` | `ALTER COLUMN ... TYPE BIGINT` as metadata-only on a streaming table — no parquet rewrite | Public Preview |
| `02_foreach_batch.py` | `@dp.foreach_batch_sink` running `DeltaTable.merge()` — the escape hatch for OVERWRITE_BY_KEY, DELETE_INSERT, and any custom merge strategy | Public Preview (Dec 2025) |
| `03_replace_where.sql` | `FLOW REPLACE WHERE` — predicate-scoped reprocessing on every pipeline update | Private Preview (Feb 2026) |

Deploy and run:

```bash
databricks bundle run qa_demos_setup -t dev
databricks bundle run qa_demos_pipeline -t dev
```

See [`src/qa_demos/README.md`](src/qa_demos/README.md) for the per-table breakdown.

## Detailed setup

### Prerequisites

1. **A Databricks workspace** with:
   - Unity Catalog enabled
   - Serverless SQL warehouse + serverless compute for pipelines/jobs
   - Permission to create catalogs, schemas, secrets, UC connections
2. **A SQL Server 2019+ instance** reachable from the workspace, hosting database `DemoDB`. See [`sql_server/INSTANCE.md`](sql_server/INSTANCE.md) for the setup template.
3. **Databricks CLI** v0.220+ authenticated:
   ```bash
   databricks auth login --host https://<your-workspace>.cloud.databricks.com
   ```
4. **Python 3.11+** with dev dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

### Configuration placeholders

Edit `databricks.yml` and replace these:

| Variable | Placeholder | Set to |
|---|---|---|
| `workspace.host` | `<your-workspace>.cloud.databricks.com` | Your workspace URL |
| `notification_email` | `<your-email@your-org.com>` | Email for pipeline failure notifications |
| `catalog_owner` | `<your-email@your-org.com>` | Catalog owner (carried as a UC tag) |
| `sqlserver_host` | `<SQLSERVER_HOST>` | Your SQL Server hostname / IP |
| `prod` target `root_path` | `<YOUR_WORKSPACE_USER>` | Your workspace user for prod deploys |

### Full setup flow

1. `databricks bundle validate -t dev`
2. `databricks bundle deploy -t dev`
3. **Create the UC connection to SQL Server** (one-time, see `resources/connections/README.md` for the SDK snippet — UC connections are workspace-level and not declared as bundle resources, by design).
4. `databricks bundle run sqlserver_setup -t dev` — runs the 5-task Job that creates the `crm` schema, seeds 500 Consultoras with deterministic tier/region distributions, backdates 50 tier transitions, then full-refreshes the main SDP pipeline.
5. **Seed the Pedidos volume**:
   ```bash
   python -m src.seed.generate_pedidos_parquet \
       --upload-to /Volumes/directsales_dev/bronze/lz/pedidos \
       --seed 42
   ```
6. `databricks bundle run sdp_main -t dev` — reads bronze, builds silver, runs AutoCDC into gold.
7. `databricks bundle run governance_setup -t dev` — creates `acl_consultora`, the row filter, the column mask, and attaches them to `gold.dim_consultora`. By default nobody is granted; INSERT into `acl_consultora` to grant region + CPF visibility.

### Run the tests

```bash
export DEMO_TEST_CATALOG=directsales_dev
pytest src/tests/ -v
```

Tests run against serverless via `databricks-connect`. They verify AutoCDC SCD2 history, the `comissao_pct` UDF, the historic-tier MV consistency, and the RLS + masking behaviour.

## GitHub Actions

Three workflows in `.github/workflows/`:

| Workflow | Trigger | Job |
|---|---|---|
| `pr-validate.yml` | PR open / push | `bundle validate -t dev`, sqlfluff lint, gitleaks secret scan |
| `deploy-dev.yml` | `workflow_dispatch` | Manual branch deploy to `directsales_dev` for preview |
| `deploy-prod.yml` | push to `main` | Deploy to `directsales_prod` (production target) |

See [`.github/ACTIONS.md`](.github/ACTIONS.md) for the auth setup (PAT vs service-principal OAuth M2M).

## Resetting the demo (dev only)

```bash
make reset               # full reset: drop schemas + redeploy + rebuild medallion (~5-8 min)
make reset-sql-server    # surgical: truncate + re-seed crm.consultoras only
make reset-volume        # surgical: wipe + regenerate the Pedidos landing zone only
```

Every reset script hardcodes `target=dev` and refuses to run against prod. See [`scripts/README.md`](scripts/README.md) for the per-script breakdown and safety guarantees.

## License

Provided as-is for evaluation purposes. No warranty.
