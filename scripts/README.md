# scripts/

One-off helper scripts for demo prep, demo reset, and live demo beats. **Not**
part of any SDP pipeline — these are local Python entry points run from the
developer machine.

| Script | Purpose | Slice |
|--------|---------|-------|
| `demo_schema_evolution.py` | Drop a `canal_origem` parquet into bronze; watch Auto Loader widen | 09 |
| `reset_sql_server.py` | Truncate + re-seed `crm.consultoras` + run `sqlserver_setup` Job | 12 |
| `reset_volume.py` | Wipe `/Volumes/.../bronze/lz/pedidos/` and regenerate the canonical 50k Pedido set | 12 |
| `reset_databricks.py` | DROP SCHEMA cascade + redeploy + rebuild medallion + acl_consultora baseline | 12 |

The reset scripts are also wired into the repo-root `Makefile`:

```bash
make reset               # full reset — sql-server -> volume -> databricks (~3 min)
make reset-sql-server    # ~30s (warm)
make reset-volume        # ~10s
make reset-databricks    # ~2 min
```

## Demo reset (Slice 12)

The three reset scripts are designed to compose. Run individually for fast
recovery from a single fat-fingered demo step, or run `make reset` for a full
clean rebuild between rehearsals. **All scripts hardcode `target=dev`** and
refuse to start with any other target (PRD AC 36).

### Safety guarantees

- **Hard-coded to `dev`.** Every script's `--target` argparse choice is
  `{'dev'}`; `--target-catalog directsales_prod` is also rejected. There is no
  way to use these scripts to reset `directsales_prod` without editing the source.
- **`make reset-databricks` is interactively guarded.** Without `--yes`, the
  script prints a 5-second countdown before issuing the first `DROP SCHEMA`.
  The Makefile target passes `--yes` for unattended use; run the script
  directly to opt into the prompt.
- **No secrets read locally** beyond the SQL Server SA credentials needed by
  `reset_sql_server.py` (read from `$SQLSERVER_SA_USER` / `$SQLSERVER_SA_PASSWORD`
  env vars). The bundle Job reads the same values from workspace secret scope
  `directsales-demo` and is the canonical source — env var mode is just for the
  local TRUNCATE.
- **Reset is local-only.** No `.github/workflows/` workflow exists for any
  reset target. `make reset` cannot run against `prod`.

### `reset-sql-server`

```bash
make reset-sql-server
# or, with the interactive prompt:
python scripts/reset_sql_server.py
```

Pipeline:

1. `TRUNCATE crm.consultoras` on the demo SQL Server (`<SQLSERVER_HOST>:1433`).
   TRUNCATE on a CHANGE_TRACKING-enabled table also resets the table's CT
   version, giving the LFC ingestion gateway a clean cursor.
2. `databricks bundle run sqlserver_setup -t dev` — the 5-task Job that
   re-runs `create_uc_connection`, `phase_1_seed`, `lfc_initial_sync`,
   `phase_2_transitions`, `lfc_post_transition_sync`. Two LFC syncs around
   `phase_2_transitions` produce the two distinct change events Slice 06's
   AutoCDC SCD2 needs to build hero #42's bronze→prata SCD2 history.
3. Verify: 500 consultoras, hero #42 at `tier='prata'` with
   `updated_at='2026-02-01'`, 48 backdated transitioned rows at `2025-11-01`
   (50 candidates minus the 2 seeded at diamante; diamante is the top tier).

> **Slice 04 idempotency drift fixed.** With the data_cadastro=2025-09-01 pin
> in `src/seed/generate_consultoras_sql.py` and the `updated_at < '2025-11-01'`
> guard in `sql_server/ddl/04_apply_tier_transitions.sql`, re-running script 04
> is a no-op. The bulk-tier promotion now lands on the same 48 rows every reset.

Required env vars:

```bash
export SQLSERVER_SA_USER=SA
export SQLSERVER_SA_PASSWORD=<see sql_server/INSTANCE.md>
```

### `reset-volume`

```bash
make reset-volume
# or, with the interactive prompt:
python scripts/reset_volume.py
```

Pipeline:

1. List every file in `/Volumes/directsales_dev/bronze/lz/pedidos/` and delete
   them (including any `pedidos_canal_origem_*.parquet` smoke artefacts left
   by the Slice 09 live beat).
2. Regenerate the canonical 50-file 50 000-row Pedido set via
   `src.seed.generate_pedidos_parquet.generate(seed=42, n_pedidos=50_000)`
   into a tempdir, then upload each file to the volume.

This script does **not** trigger the SDP pipeline. The new files sit in the
volume until `reset_databricks.py` runs `full_refresh: true` on the SDP
pipeline. That separation keeps `reset-volume` fast (~10s) and lets it be a
one-shot recovery tool when only the parquet landing zone got dirty.

### `reset-databricks`

```bash
make reset-databricks
# or, with the interactive prompt:
python scripts/reset_databricks.py
```

Pipeline (9 steps):

1. **Assert `--target=dev`** and prompt for 5-second confirmation.
2. **DROP SCHEMA bronze | silver | gold | _lfc_staging CASCADE** on
   `directsales_dev`. Catalog + mandatory `RemoveAfter` / `Owner` tags persist.
   The UC connection `directsales_sqlserver_conn` is **not** dropped (Slice 04
   cross-reference: `bundle deploy` validates connection existence eagerly,
   so dropping it would brick the next deploy).
3. `databricks bundle deploy -t dev` — recreates schemas + pipeline + Job
   resources.
4. `databricks bundle run gold_setup -t dev` — recreates `gold.comissao_pct`
   UDF. **Must run before the SDP pipeline** (Slice 07 cross-reference: MV2
   references the UDF and SDP compilation will fail with "function not found"
   otherwise).
5. **Pre-warm the LFC gateway.** Polls `list_pipeline_events` for the
   gateway's "Schema Exploration COMPLETED" message before triggering
   ingestion. Slice 04 cross-reference: gateway cold start is ~5 min on a
   fresh workspace, instant on re-deploys.
6. `databricks bundle run sqlserver_setup -t dev` — the 5-task Job. Same
   semantics as `reset-sql-server` but the bundle deploy in step 3 already
   wiped the post-Slice-08 governance attachments, so this is a re-snapshot.
7. **Trigger `sdp_main` with `full_refresh=true`** and wait for
   completion. Recreates `silver.*` and `gold.*` (dim_consultora, fact_pedido,
   MV1, MV2). At this point `gold.dim_consultora` exists but has no row
   filter or column mask attached.
8. `databricks bundle run governance_setup -t dev` — recreates
   `gold.acl_consultora`, `gold.consultora_rls`, `gold.mask_cpf`, and re-attaches
   ROW FILTER + cpf MASK to `gold.dim_consultora`. **Must run after step 7**
   (Slice 08 cross-reference: ALTER statements need dim_consultora to exist).
9. `INSERT INTO gold.acl_consultora` baseline admin row + verify post-state.
   PRD AC 41: one admin row + zero rows for `current_user()` so the demo's
   "Malcoln has no access" starting state is reproducible.

Verification queries at the end:

- `bronze.consultoras_raw` row count == 500
- `bronze.pedidos_raw` row count == 50 000
- `bronze.pedidos_raw` schema does **not** include `canal_origem` (Slice 09
  cross-reference: rolled-back pre-evolution baseline)
- `gold.fact_pedido` > 0 rows
- `gold.report_comissao_pedido_tier_historico` < `gold.fact_pedido` count
  (Slice 07 cross-reference: legitimate gap, not equality)
- `SELECT count(*) FROM gold.dim_consultora` == 0 for `current_user()`
  (the row filter is in effect with no `acl_consultora` grant)

### `make reset` order

```
reset-sql-server -> reset-volume -> reset-databricks
```

SQL Server first so LFC has clean source state when its gateway re-snapshots.
Volume next so the parquet landing zone is fresh. Databricks last so the SDP
pipeline's `full_refresh` re-snapshots from both clean sources.

## Live beat: schema evolution (Slice 09)

The "Auto Loader added a column in 30 seconds with no code change" beat. Drops
a small (~100 row) parquet file with a NEW `canal_origem` column into the
bronze landing volume. Auto Loader (configured with
`schemaEvolutionMode = 'addNewColumns'` in `src/sdp/00_bronze.sql`) widens
`bronze.pedidos_raw` automatically; `silver.pedidos` propagates on the same
update because its SELECT uses `* EXCEPT (...)` form.

### Prerequisite

The `sdp-main` SDP pipeline must be deployable and runnable for the
target catalog. Bundle deploy + at least one prior pipeline run is required so
the catalog/schema/volume scaffolding exists. This script does **not** trigger
the pipeline itself — it just lands the file.

### Run

```bash
# Default: drops into /Volumes/directsales_dev/bronze/lz/pedidos with a fresh seed
python scripts/demo_schema_evolution.py

# Override the catalog (e.g. for a personal scratch catalog)
python scripts/demo_schema_evolution.py --target-catalog my_scratch_catalog

# Reproducible drop for smoke tests (same seed -> same parquet bytes)
python scripts/demo_schema_evolution.py --seed 909

# Local-only, no upload
python scripts/demo_schema_evolution.py --dry-run
```

Auth: uses your active Databricks workspace profile via `WorkspaceClient()`
(set `DATABRICKS_CONFIG_PROFILE` or rely on the default profile).

### Expected timing

~30 seconds from file upload to visible widening in `bronze.pedidos_raw`,
assuming the SDP pipeline is in CONTINUOUS mode or gets triggered shortly
after. For triggered pipelines, run the pipeline manually after the upload.

### Manual smoke verification

Run through this checklist after every demo rehearsal:

- [ ] `databricks pipelines list -o json | grep sdp-main` shows the pipeline.
- [ ] Trigger the pipeline (or wait for the next continuous tick) **before**
      running the script — confirms baseline state.
- [ ] Run `python scripts/demo_schema_evolution.py`. Output prints:
      `Generated pedidos_canal_origem_<date>_<seed>.parquet (100 rows). Canal distribution: {'app': ~60, 'web': ~30, 'whatsapp': ~10}.`
      followed by `Uploaded -> /Volumes/.../pedidos/...`.
- [ ] Trigger the SDP pipeline (the upload itself does not auto-trigger a
      triggered pipeline; for continuous mode, just wait).
- [ ] After the run completes, query bronze:

      ```sql
      SELECT canal_origem, COUNT(*) AS n
      FROM directsales_dev.bronze.pedidos_raw
      GROUP BY canal_origem
      ORDER BY n DESC;
      ```

      Expect: ~30 000 rows with `canal_origem IS NULL` (pre-evolution
      baseline) plus three rows for `app`, `web`, `whatsapp` summing to 100.

- [ ] Query silver to confirm propagation:

      ```sql
      SELECT canal_origem, COUNT(*) AS n
      FROM directsales_dev.silver.pedidos
      GROUP BY canal_origem
      ORDER BY n DESC;
      ```

      Expect the same shape (the silver column should be present and
      populated for the new rows).

- [ ] `DESCRIBE directsales_dev.bronze.pedidos_raw` and `DESCRIBE directsales_dev.silver.pedidos`
      both show `canal_origem STRING` in the column list.

If any step fails, the most common cause is the pipeline not having been
triggered after the file landed. The Auto Loader source is event-driven only
when configured with file notification — for the demo's volume-based source,
the pipeline must run for the file to be observed.

### Resetting between runs

`make reset-volume` wipes `/Volumes/.../pedidos/` and regenerates the main
50 000-row parquet set. `make reset-databricks` then rolls the bronze schema
back via `DROP SCHEMA CASCADE` -> `bundle deploy` -> SDP `full_refresh`. After
both have run, the `canal_origem` column is gone from bronze and the schema
reverts to the non-evolved baseline. `reset_databricks.py` includes a
post-reset assertion that `canal_origem` is **not** in
`DESCRIBE bronze.pedidos_raw`.

## Prod first-time bootstrap (Slice 13 follow-up)

After Slice 13's schema split, prod has its own `crm_prod.consultoras` table
on SQL Server and its own `directsales_prod.*` catalog in Databricks. To stand prod
up for the first time, four pre-conditions must be satisfied **before**
running `bundle run sqlserver_setup -t prod`. They are NOT part of the Job
DAG and skipping any of them causes a cascading SDP failure several minutes
into the run (recoverable via `databricks jobs repair-run --rerun-all-failed-tasks`,
but slower than getting the order right the first time).

### Ladder

1. **Migrate or create the SQL Server schema.**

   For an existing demo with a `crm` schema in `DemoDB`:
   ```sql
   CREATE SCHEMA crm_dev;
   ALTER SCHEMA crm_dev TRANSFER crm.consultoras;
   DROP SCHEMA crm;
   ```
   `ALTER SCHEMA ... TRANSFER` preserves the table's `object_id`, so the LFC
   gateway's Change Tracking cursor follows without needing a `full_refresh`.
   `sp_rename` does NOT work for SQL Server schemas — use the TRANSFER pattern.

   For prod (fresh schema):
   ```bash
   # Read sql_server/ddl/02_create_tables.sql, substitute {{schema}} -> crm_prod,
   # split on GO batch separators, execute via python-tds (idempotent).
   ```
   This creates `crm_prod.consultoras` + indexes + enables Change Tracking.

2. **Seed the prod volume with pedidos parquet.**

   `bundle deploy` creates `/Volumes/directsales_prod/bronze/lz/pedidos/` empty.
   `scripts/reset_volume.py` is intentionally locked to `--target dev` (PRD
   AC 36), so it can't help here. The minimal viable path is to copy from
   dev's volume via the Databricks Files SDK:
   ```python
   import io
   from databricks.sdk import WorkspaceClient
   w = WorkspaceClient()
   src = "/Volumes/directsales_dev/bronze/lz/pedidos"
   dst = "/Volumes/directsales_prod/bronze/lz/pedidos"
   for e in w.files.list_directory_contents(src):
       if e.is_directory:
           continue
       data = w.files.download(file_path=e.path).contents.read()
       w.files.upload(
           file_path=f"{dst}/{e.path.rsplit('/', 1)[-1]}",
           contents=io.BytesIO(data),
           overwrite=True,
       )
   ```
   Note `io.BytesIO(...)` — the SDK's `upload()` requires a file-like object,
   not raw bytes (`AttributeError: 'bytes' object has no attribute 'seekable'`).

3. **Create the gold-layer UDF.**
   ```bash
   databricks bundle run gold_setup -t prod
   ```
   Creates `directsales_prod.gold.comissao_pct(tier)` — the UDF referenced by MV2.
   Idempotent (`CREATE OR REPLACE FUNCTION`), takes ~30s. Without this, SDP
   compiles bronze/silver/dim/fact successfully but fails on MV2 with
   `UNRESOLVED_ROUTINE: Cannot resolve routine 'directsales_prod.gold.comissao_pct'`.
   UDF resolution happens at MV planning time, so creating the UDF mid-run
   doesn't help the current pipeline update — you must repair-run.

4. **Run the SQL Server setup Job against prod.**
   ```bash
   databricks bundle run sqlserver_setup -t prod
   ```
   The 8-task DAG (create_uc_connection -> phase_0_truncate -> phase_1_seed ->
   lfc_initial_sync -> sdp_initial_refresh -> phase_2_transitions (+180s wait)
   -> lfc_post_transition_sync -> sdp_incremental_refresh) lands the full
   500-row consultoras + tier-transition state in `directsales_prod`, including
   hero #42's closed-bronze + open-prata SCD2 pair in `gold.dim_consultora`.

   **Never run this against prod again** after the initial bootstrap. Prod
   is meant to be stable; only dev runs the destructive reset cycles.

### Expected end state

After the four steps, `directsales_prod` should match `directsales_dev`:
- `bronze.consultoras_raw`: 500 rows
- `bronze.pedidos_raw`: 50000 rows
- `silver.consultoras`: 548 rows (insert + update_postimage CDF events)
- `silver.pedidos`: 49992 rows (~50000 minus expectation drops)
- `gold.dim_consultora`: 548 SCD2 rows; hero #42 has exactly 2 (closed bronze + open prata)
- `gold.fact_pedido`: 49992 rows
- `gold.report_comissao_pedido_tier_historico`: ~31677 rows (strictly less than fact_pedido — pedidos predating their consultora's SCD2 history)

### Recovery: if step 4 was fired prematurely

If you run `bundle run sqlserver_setup -t prod` before satisfying conditions
1-3, the Job fails at `sdp_initial_refresh`. Successful tasks (create_uc_connection,
phase_0_truncate, phase_1_seed, lfc_initial_sync) are preserved. After fixing
the missing pre-condition, run:

```bash
databricks jobs repair-run <run_id> --rerun-all-failed-tasks
```

Databricks Jobs preserve task-level success state across repairs, so the
already-succeeded steps don't re-execute. Total recovery time after fix:
~7-9 minutes (vs ~13 minutes for a fresh run).
