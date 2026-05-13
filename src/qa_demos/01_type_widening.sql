-- Pain Point 1 — Type Widening on streaming tables.
--
-- Streaming table built from the toy source. Type Widening is enabled at the
-- pipeline level via `pipelines.enableTypeWidening = true` in the pipeline
-- configuration; this table also declares it explicitly via TBLPROPERTIES so
-- the property is visible in the UI.
--
-- After the first pipeline run, you can widen a column live:
--   ALTER TABLE directsales_prod.qa_demos.consultoras_widening
--     ALTER COLUMN consultora_id TYPE BIGINT;
-- The change is metadata-only — no parquet rewrite.

CREATE OR REFRESH STREAMING TABLE consultoras_widening
COMMENT
'Pain 1 — Type Widening: streaming table that survives numeric/string type
widenings without rewriting underlying parquet.'
TBLPROPERTIES (
  'quality' = 'demo',
  'delta.enableTypeWidening' = 'true'
)
AS SELECT
  consultora_id,
  nome,
  regiao,
  tier,
  valor_total,
  data_pedido
FROM STREAM (${target_catalog}.qa_demos.consultoras_toy_source);
