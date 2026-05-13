-- Pain Point 3 — REPLACE WHERE flow on a streaming table (PrPr Feb 2026).
--
-- IMPORTANT: REPLACE WHERE flows are Private Preview. The workspace must be
-- enrolled (contact your Databricks rep) AND the pipeline must be on the
-- PREVIEW channel (qa_demos pipeline is). If not enrolled, this flow will
-- fail at pipeline-update time with a clear error; comment this file out
-- and redeploy.
--
-- Predicate: `data_pedido >= '2026-02-01'`. On every pipeline update, the
-- engine deletes rows in the target matching the predicate, re-evaluates
-- the SELECT for that same range, and inserts the new results. Rows
-- outside the predicate (data_pedido < '2026-02-01') stay untouched.
--
-- Backfill of older ranges goes through the pipeline update API with
-- `replace_where_overrides` — see qa-pain-points.html#pain3.

CREATE STREAMING TABLE consultoras_replace_where_recent
COMMENT 'Pain 3: REPLACE WHERE flow — re-evaluates only February+ rows on each pipeline run. Older rows preserved.'
TBLPROPERTIES ('quality' = 'demo')
FLOW REPLACE WHERE data_pedido >= TIMESTAMP'2026-02-01' BY NAME
SELECT
  consultora_id,
  nome,
  regiao,
  tier,
  valor_total,
  data_pedido
FROM ${target_catalog}.qa_demos.consultoras_toy_source;
