# Databricks notebook source
# MAGIC %md
# MAGIC # Q&A Demos — one-time source setup
# MAGIC
# MAGIC Creates `${target_catalog}.qa_demos.consultoras_toy_source` (Delta, CDF
# MAGIC enabled) with 5 toy rows. Run once before the `qa_demos` pipeline.
# MAGIC
# MAGIC Idempotent — `CREATE TABLE IF NOT EXISTS` + idempotent `MERGE` re-seed.

# COMMAND ----------

dbutils.widgets.text("target_catalog", "directsales_dev", "Target catalog")
target_catalog = dbutils.widgets.get("target_catalog")
assert target_catalog in {"directsales_dev", "directsales_prod"}, f"unexpected target_catalog: {target_catalog}"

source_table = f"{target_catalog}.qa_demos.consultoras_toy_source"
print(f"Seeding source table: {source_table}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.qa_demos")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {source_table} (
  consultora_id INT,
  nome STRING,
  regiao STRING,
  tier STRING,
  valor_total DOUBLE,
  data_pedido TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# COMMAND ----------

# Idempotent seed via MERGE — re-running this notebook doesn't duplicate rows
spark.sql(f"""
MERGE INTO {source_table} AS t
USING (SELECT * FROM VALUES
  (1, 'Ana',     'Sul',          'bronze',  100.50, TIMESTAMP'2026-01-01'),
  (2, 'Beatriz', 'Sudeste',      'prata',   250.00, TIMESTAMP'2026-01-15'),
  (3, 'Camila',  'Nordeste',     'bronze',   80.00, TIMESTAMP'2026-02-01'),
  (4, 'Daniela', 'Sul',          'ouro',    500.00, TIMESTAMP'2026-02-10'),
  (5, 'Eduarda', 'Centro-Oeste', 'bronze',  150.00, TIMESTAMP'2026-03-01')
  AS s(consultora_id, nome, regiao, tier, valor_total, data_pedido)) AS s
ON t.consultora_id = s.consultora_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {source_table} ORDER BY consultora_id"))
print(f"OK: {source_table} ready with 5 rows + CDF enabled. Trigger the qa_demos pipeline next.")
