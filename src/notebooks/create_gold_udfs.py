# Databricks notebook source
# MAGIC %md
# MAGIC # Create gold-layer UDFs — Slice 07
# MAGIC
# MAGIC One-shot, idempotent task that creates SQL UDFs in `${target_catalog}.gold`.
# MAGIC SDP pipeline files only accept `CREATE STREAMING TABLE`,
# MAGIC `CREATE MATERIALIZED VIEW`, `APPLY CHANGES INTO`, and `SET` statements
# MAGIC ("not supported in a DLT pipeline" otherwise) — so UDFs that pipeline
# MAGIC objects depend on must be created out-of-band, before the pipeline runs.
# MAGIC
# MAGIC Currently creates one UDF:
# MAGIC
# MAGIC * `gold.comissao_pct(tier STRING) RETURNS DECIMAL(5,4)` — single
# MAGIC   declarative source of truth for the tier -> commission rate mapping
# MAGIC   used by `gold.report_comissao_pedido_tier_historico` (MV2). The five
# MAGIC   documented tiers map to explicit rates; unknown tiers return NULL so
# MAGIC   drift surfaces as missing-data downstream rather than silent zeros.
# MAGIC
# MAGIC `CREATE OR REPLACE FUNCTION` is idempotent — safe to re-run on every
# MAGIC reset, and updates the function body in place if the rates ever change.

# COMMAND ----------

# DBTITLE 1,Read parameters
dbutils.widgets.text("target_catalog", "")
target_catalog = dbutils.widgets.get("target_catalog")
if not target_catalog:
    raise ValueError("target_catalog widget is required")

print(f"Target catalog: {target_catalog}")

# COMMAND ----------

# DBTITLE 1,Create gold.comissao_pct
# Tier rates documented in src/sdp/03_gold_reports.sql header. Keep this body
# in sync with src/tests/test_comissao_pct.py's TIER_RATES — the test is the
# CI gate against accidental drift.
comissao_pct_sql = f"""
CREATE OR REPLACE FUNCTION {target_catalog}.gold.comissao_pct(tier STRING)
RETURNS DECIMAL(5, 4)
COMMENT 'Tier -> commission rate. Single declarative source of truth shared by fact_pedido derivations and the historic comissao MV. Unknown tiers return NULL so drift surfaces as missing-data downstream.'
RETURN CASE tier
  WHEN 'semente' THEN 0.0200
  WHEN 'bronze' THEN 0.0300
  WHEN 'prata' THEN 0.0500
  WHEN 'ouro' THEN 0.0800
  WHEN 'diamante' THEN 0.1200
END
"""

print("Executing CREATE OR REPLACE FUNCTION gold.comissao_pct ...")
spark.sql(comissao_pct_sql)
print("OK.")

# COMMAND ----------

# DBTITLE 1,Verify
for tier in ("semente", "bronze", "prata", "ouro", "diamante", "platina_invalida"):
    row = spark.sql(
        f"SELECT {target_catalog}.gold.comissao_pct('{tier}') AS rate"
    ).collect()[0]
    print(f"comissao_pct({tier!r}) = {row['rate']}")
