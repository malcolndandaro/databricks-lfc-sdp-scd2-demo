-- Gold: dim / fact built via SDP AutoCDC.
--
-- gold.dim_consultora (SCD TYPE 2) — every change to tier, regiao,
-- email, or ativo closes the previous row's __END_AT and opens a new
-- row. Changes to other columns update in place.
--
-- gold.fact_pedido (SCD TYPE 1) — latest row per pedido_id ordered by
-- updated_at. Late-arriving updates overwrite; no history kept.
--
-- APPLY CHANGES INTO and AUTO CDC INTO are both valid; we use APPLY
-- CHANGES INTO. Schema is inferred from the SEQUENCE BY-ordered source
-- and AutoCDC adds __START_AT / __END_AT automatically for SCD2.

CREATE OR REFRESH STREAMING TABLE gold.dim_consultora
COMMENT
'SCD2 dim of Consultoras built via AutoCDC. Tracks tier, regiao, email,
ativo. __START_AT and __END_AT bracket each validity window; open rows
have __END_AT IS NULL.'
TBLPROPERTIES (
  'quality' = 'gold'
);

APPLY CHANGES INTO gold.dim_consultora
FROM STREAM (silver.consultoras)
KEYS (consultora_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2
TRACK HISTORY ON tier, regiao, email, ativo;


CREATE OR REFRESH STREAMING TABLE gold.fact_pedido
COMMENT
'SCD1 fact of Pedidos built via AutoCDC. Latest row per pedido_id
ordered by updated_at. Joined to dim_consultora in MV2 for
as-of-pedido-date tier attribution.'
TBLPROPERTIES (
  'quality' = 'gold',
  'delta.feature.timestampNtz' = 'supported'
);

APPLY CHANGES INTO gold.fact_pedido
FROM STREAM (silver.pedidos)
KEYS (pedido_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;
