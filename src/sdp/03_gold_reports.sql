-- Gold reports: two materialized views over dim_consultora + fact_pedido.
--
-- gold.report_vendas_regiao_mes_atual
--   Current-snapshot rollup. Joins fact_pedido to dim_consultora on
--   __END_AT IS NULL — every Pedido is attributed to each Consultora's
--   current tier.
--
-- gold.report_comissao_pedido_tier_historico
--   As-of join: each Pedido is attributed to the tier active at
--   data_pedido, i.e. where data_pedido falls inside the SCD2 validity
--   window [__START_AT, __END_AT). comissao_devida = valor_total
--   * comissao_pct(historic tier).
--
-- UDF dependency: gold.comissao_pct(tier STRING) RETURNS DECIMAL(5,4)
-- is created by the gold_setup Job before this pipeline runs (SDP
-- pipeline files don't accept CREATE FUNCTION). Idempotent via
-- CREATE OR REPLACE.
--
-- Type note: silver.consultoras.updated_at is TIMESTAMP; silver.pedidos.
-- data_pedido is TIMESTAMP_NTZ. The as-of join casts data_pedido to
-- TIMESTAMP so the comparison stays in one type system.

-- ---------- MV1 — current-snapshot rollup ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.report_vendas_regiao_mes_atual
COMMENT
'Vendas rollup by regiao, current tier, and month. Joins fact_pedido
to dim_consultora on __END_AT IS NULL so each Pedido is attributed to
the Consultora''s current tier.'
TBLPROPERTIES (
  'quality' = 'gold'
)
AS
SELECT
  d.regiao,
  d.tier AS tier_atual,
  date_trunc('month', f.data_pedido) AS mes,
  sum(f.valor_total) AS total_vendas,
  count(f.pedido_id) AS qtde_pedidos
FROM gold.fact_pedido AS f
INNER JOIN gold.dim_consultora AS d
  ON
    f.consultora_id = d.consultora_id
    AND d.__END_AT IS NULL
GROUP BY d.regiao, d.tier, date_trunc('month', f.data_pedido);


-- ---------- MV2 — as-of-pedido-date tier attribution ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.report_comissao_pedido_tier_historico
COMMENT
'Per-Pedido tier attribution as-of data_pedido. Joins fact_pedido to
dim_consultora where data_pedido falls inside the SCD2 validity window
[__START_AT, __END_AT). comissao_devida = valor_total * comissao_pct
(tier at the time of the Pedido).'
TBLPROPERTIES (
  'quality' = 'gold',
  'delta.feature.timestampNtz' = 'supported'
)
AS
SELECT
  f.pedido_id,
  f.consultora_id,
  f.data_pedido,
  f.valor_total,
  d.tier AS tier_no_momento_do_pedido,
  f.valor_total * ${target_catalog}.gold.comissao_pct(d.tier) AS comissao_devida
FROM gold.fact_pedido AS f
INNER JOIN gold.dim_consultora AS d
  ON
    f.consultora_id = d.consultora_id
    AND cast(f.data_pedido AS TIMESTAMP) >= d.__START_AT
    AND (cast(f.data_pedido AS TIMESTAMP) < d.__END_AT OR d.__END_AT IS NULL);
