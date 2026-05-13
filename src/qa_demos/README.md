# `qa_demos` — pipeline for the demo's Q&A pain-point answers

A standalone SDP pipeline that demonstrates three pain points the main demo doesn't cover live: type widening on streaming tables, custom merge strategies via ForEachBatch sink, and predicate-scoped reprocessing via REPLACE WHERE.

## What's in it

| File | Pain point | Status | What it shows |
|---|---|---|---|
| `01_type_widening.sql` | **Pain 1** — Schema type changes break streaming tables | Public Preview (GA delayed 2-4 weeks) | Streaming table with `delta.enableTypeWidening = true`. After the first pipeline run, `ALTER TABLE ... ALTER COLUMN consultora_id TYPE BIGINT` is metadata-only — no parquet rewrite. |
| `02_foreach_batch.py` | **Pain 2** — Custom merge strategies — APPLY CHANGES INTO only covers basic SCD patterns | Public Preview (Dec 2025) | `@dp.foreach_batch_sink` running `DeltaTable.merge().whenMatchedUpdateAll().whenNotMatchedInsertAll().whenNotMatchedBySourceDelete()` — OVERWRITE_BY_KEY in 4 lines. MERGE INTO inside the sink is supported in PuPr. |
| `03_replace_where.sql` | **Pain 3** — Partial reprocessing by date range | Private Preview (Feb 2026) | `CREATE STREAMING TABLE … FLOW REPLACE WHERE data_pedido >= … BY NAME SELECT …`. Every pipeline run re-evaluates only the predicate range; older rows preserved. **Requires workspace enrollment** — fails at update time if not enrolled. |

Pain 4 (serverless limits) doesn't fit a "demo it working" frame — that one stays talk-track only.

## How to deploy + run

Two prerequisites and one trigger:

```bash
# 1. Deploy the pipeline + setup Job to the target catalog
databricks bundle deploy -t prod   # or -t dev

# 2. Seed the toy source table (one-time, idempotent — re-running is safe)
databricks bundle run qa_demos_setup -t prod

# 3. Trigger the pipeline to materialize the demo tables
databricks bundle run qa_demos_pipeline -t prod
```

After step 3, these tables exist under `${target_catalog}.qa_demos`:

| Table | Created by | Rows | Purpose |
|---|---|---|---|
| `consultoras_toy_source` | Setup Job | 5 | Toy source — CDF enabled, fed by `MERGE` so re-runs are idempotent |
| `consultoras_widening` | `01_type_widening.sql` | 5 | Streaming target with type widening enabled |
| `consultoras_overwrite_by_key_sink` | `02_foreach_batch.py` | (flow def) | The foreach_batch_sink itself; references `consultoras_merged_external` |
| `consultoras_merged_external` | `02_foreach_batch.py` (side effect) | 5 | Where ForEachBatch actually MERGE-INTOs |
| `consultoras_replace_where_recent` | `03_replace_where.sql` | 3 | February-onward rows only (predicate-scoped). Won't exist if PrPr not enrolled. |

## Pipeline configuration highlights

- **Channel:** `PREVIEW` — required for both ForEachBatch sink (PuPr) and REPLACE WHERE flow (PrPr).
- **Compute:** Serverless + Photon.
- **`root_path`:** `../../src/qa_demos`. Workspace pipeline UI shows the source-tree cleanly under one anchor.
- **Pipeline-level Type Widening:** `pipelines.enableTypeWidening: 'true'` in the configuration. Every streaming table the pipeline creates gets `delta.enableTypeWidening = true` automatically — saves declaring it per-table.

See `resources/pipelines/qa_demos.yml` for the full bundle definition.

## Demo flow during Q&A

When a pain point comes up:

1. **Switch to the workspace UI**, open `[prod] qa-demos` pipeline graph.
2. **Click the flow** corresponding to the pain (consultoras_widening, consultoras_overwrite_by_key_sink, consultoras_replace_where_recent).
3. **Open the source-files tab** and walk the 20-30 lines of declarative code on screen.
4. **For Pain 1 specifically**, you can also run the live widening from a SQL editor:
   ```sql
   DESCRIBE directsales_prod.qa_demos.consultoras_widening;
   ALTER TABLE directsales_prod.qa_demos.consultoras_widening
     ALTER COLUMN consultora_id TYPE BIGINT;
   DESCRIBE directsales_prod.qa_demos.consultoras_widening;
   DESCRIBE HISTORY directsales_prod.qa_demos.consultoras_widening;
   ```
   The third `DESCRIBE` shows `consultora_id BIGINT`; `DESCRIBE HISTORY` proves it was metadata-only.

## Failure mode to know

If the workspace **isn't enrolled in REPLACE WHERE flow PrPr**, the third flow (`consultoras_replace_where_recent`) fails at pipeline update time with a clear "feature not enabled" or "REPLACE WHERE syntax not recognized" error. The other two flows still succeed.

**Recovery:** comment out / `git rm src/qa_demos/03_replace_where.sql`, redeploy, retrigger. Two-feature demo still works.

## Source-files glossary (for the curious during Q&A)

- `consultoras_toy_source` is in `bronze`-equivalent role: 5 rows, CDF enabled, MERGE-seeded. Setup notebook creates it; pipeline never modifies it.
- `consultoras_widening` reads via `STREAM(...)`: append-only CDF events flow in. Type Widening lets the schema evolve without rewriting these events.
- `consultoras_overwrite_by_key_sink` is a foreach_batch_sink, not a streaming table. Pipeline doesn't track what it writes — that's by design for the "12 strategies escape hatch" use case. The actual MERGE target (`consultoras_merged_external`) is a regular Delta table created lazily inside the sink function.
- `consultoras_replace_where_recent` re-evaluates only Feb-onward rows on each pipeline update. Demo: the first run inserts 3 rows (Camila, Daniela, Eduarda); subsequent runs replace those same 3 rows from the source.

## Related artifacts

- **Pipeline + Job bundle resources**: `resources/pipelines/qa_demos.yml`, `resources/jobs/qa_demos_setup.yml`
- **Setup notebook source**: `src/notebooks/qa_demos_setup.py`
