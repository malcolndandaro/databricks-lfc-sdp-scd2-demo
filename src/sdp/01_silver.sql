-- Silver: 1:1 curated streaming tables over bronze sources.
--
-- silver.consultoras lives in 01a_silver_consultoras.py because its
-- bronze source is written by Lakeflow Connect via MERGE; a SQL
-- STREAM read fails on non-append commits. The Python sibling reads
-- bronze's Change Data Feed directly.
--
-- silver.pedidos uses SELECT * EXCEPT (...) so new bronze columns
-- flow through automatically without an SQL edit.

CREATE OR REFRESH STREAMING TABLE silver.pedidos (
  CONSTRAINT valor_positivo EXPECT (valor_total > 0) ON VIOLATION DROP ROW,
  CONSTRAINT data_passada EXPECT (data_pedido <= current_date()) ON VIOLATION DROP ROW
)
COMMENT
'Curated Pedidos — 1:1 from bronze.pedidos_raw with valor_total cast
to DECIMAL(18,2) and bronze metadata dropped. Two DROP ROW
expectations guard negative valor and future dates. Feeds AutoCDC
SCD1 in gold.fact_pedido.'
TBLPROPERTIES (
  'quality' = 'silver',
  'delta.feature.timestampNtz' = 'supported'
)
AS SELECT
  * EXCEPT (
    valor_total,
    _rescued_data,
    _ingest_file_path,
    _ingest_file_mtime,
    _ingest_ts
  ),
  cast(valor_total AS DECIMAL(18, 2)) AS valor_total
FROM STREAM (${target_catalog}.bronze.pedidos_raw);
