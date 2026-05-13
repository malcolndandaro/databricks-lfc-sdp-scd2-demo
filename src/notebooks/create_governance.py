# Databricks notebook source
# MAGIC %md
# MAGIC # Create governance assets — Slice 08
# MAGIC
# MAGIC One-shot, idempotent task that creates the RLS + column-mask
# MAGIC governance objects on `${target_catalog}.gold`:
# MAGIC
# MAGIC * `gold.acl_consultora` — Delta control table (`email`, `regiao`, `cpf`).
# MAGIC * `gold.consultora_rls(regiao STRING) RETURNS BOOLEAN` — row filter
# MAGIC   reading `acl_consultora` for `current_user()`.
# MAGIC * `gold.mask_cpf(cpf STRING) RETURNS STRING` — column mask: returns
# MAGIC   the raw `cpf` only when `current_user()` has a matching `acl_consultora`
# MAGIC   row whose `cpf` equals the column value; otherwise returns
# MAGIC   `***.***.***-**`.
# MAGIC * `ALTER TABLE gold.dim_consultora SET ROW FILTER consultora_rls
# MAGIC   ON (regiao)` — attach row filter.
# MAGIC * `ALTER TABLE gold.dim_consultora ALTER COLUMN cpf SET MASK
# MAGIC   mask_cpf` — attach column mask.
# MAGIC
# MAGIC SDP pipeline files only accept `CREATE STREAMING TABLE`,
# MAGIC `CREATE MATERIALIZED VIEW`, `APPLY CHANGES INTO`, and `SET` — so
# MAGIC `CREATE TABLE`/`CREATE FUNCTION`/`ALTER TABLE` for governance live
# MAGIC out-of-band in this notebook (mirrors the gold_setup pattern from
# MAGIC Slice 07's `create_gold_udfs.py`).
# MAGIC
# MAGIC ## Run order
# MAGIC
# MAGIC This notebook MUST run **after** the SDP pipeline has created
# MAGIC `gold.dim_consultora` — the two `ALTER TABLE` statements at the end
# MAGIC fail with `TABLE_OR_VIEW_NOT_FOUND` otherwise. The control table
# MAGIC and UDFs themselves can be created any time (UDF bodies are
# MAGIC text-resolved at invocation, not at CREATE).
# MAGIC
# MAGIC The Slice 12 reset path is responsible for sequencing this Job
# MAGIC after the SDP pipeline run.
# MAGIC
# MAGIC ## Idempotency
# MAGIC
# MAGIC * `CREATE TABLE IF NOT EXISTS` for `acl_consultora` — pre-existing
# MAGIC   rows are preserved across re-runs. **Reset baseline** (per
# MAGIC   PRD § "Reset re-seeds acl_consultora with a single admin row")
# MAGIC   is the responsibility of `scripts/reset_databricks.py`, not
# MAGIC   this notebook. Re-running this notebook does NOT touch
# MAGIC   existing rows.
# MAGIC * `CREATE OR REPLACE FUNCTION` — re-creating UDFs in place.
# MAGIC * `ALTER TABLE ... SET ROW FILTER` — replaces any existing
# MAGIC   filter on the target column tuple. Same for `SET MASK`.
# MAGIC
# MAGIC ## Why `acl_consultora` instead of `is_account_group_member`
# MAGIC
# MAGIC The `EXISTS(acl_consultora)` pattern is a deliberate workaround for
# MAGIC workspace group restrictions. Production swaps it for
# MAGIC `is_account_group_member('group')` — same shape, IdP-integrated.

# COMMAND ----------

# DBTITLE 1,Read parameters
dbutils.widgets.text("target_catalog", "")
target_catalog = dbutils.widgets.get("target_catalog")
if not target_catalog:
    raise ValueError("target_catalog widget is required")

print(f"Target catalog: {target_catalog}")

# COMMAND ----------

# DBTITLE 1,Create gold.acl_consultora
# Schema fixed at email/regiao/cpf. Delta default; no
# enforced PK (Delta doesn't enforce uniqueness — we accept duplicate
# (email, regiao, cpf) tuples may exist transiently between INSERT and
# the next reset).
create_acl_sql = f"""
CREATE TABLE IF NOT EXISTS {target_catalog}.gold.acl_consultora (
  email STRING COMMENT 'Workspace user email; matched against current_user() in the row filter and column mask UDFs',
  regiao STRING COMMENT 'Brazilian region the user is granted to see in dim_consultora rows. NULL means no region grant.',
  cpf STRING COMMENT 'Specific Consultora CPF the user is allowed to see unmasked. NULL means CPF is masked for this user even within their granted region.'
)
USING DELTA
COMMENT 'RLS + column-mask control table. One row per (user, region, cpf) grant. Drives consultora_rls and mask_cpf via EXISTS lookups against current_user(). Reset baseline: one admin row, no current_user() row.'
"""

print("Executing CREATE TABLE IF NOT EXISTS gold.acl_consultora ...")
spark.sql(create_acl_sql)
print("OK.")

# COMMAND ----------

# DBTITLE 1,Create gold.consultora_rls
# Row filter: TRUE iff acl_consultora has a row matching (current_user(),
# <regiao parameter>). The body uses `<param> IN (subquery)` rather
# than `EXISTS (... WHERE acl.col = <param>)` because Databricks SQL
# UDFs resolve unqualified names inside a subquery's WHERE clause
# against the inner table's columns first, which makes
# `acl.regiao = regiao` collapse to a tautology (column = column).
# IN-form keeps the parameter in the outer scope where it cannot be
# shadowed; the AC's intent ("EXISTS against acl_consultora for
# current_user() + region match") is preserved as a logical
# equivalence.
#
# Fully-qualified table reference inside the body so resolution does
# not depend on the calling session's default catalog (Slice 07
# cross-reference: SDP catalog binding does NOT cover function bodies).
create_rls_sql = f"""
CREATE OR REPLACE FUNCTION {target_catalog}.gold.consultora_rls(regiao STRING)
RETURNS BOOLEAN
COMMENT 'Row filter for gold.dim_consultora. Returns TRUE iff acl_consultora has a (current_user(), regiao) grant. Demo workaround for is_account_group_member.'
RETURN regiao IN (
  SELECT acl.regiao
  FROM {target_catalog}.gold.acl_consultora AS acl
  WHERE acl.email = current_user()
)
"""

print("Executing CREATE OR REPLACE FUNCTION gold.consultora_rls ...")
spark.sql(create_rls_sql)
print("OK.")

# COMMAND ----------

# DBTITLE 1,Create gold.mask_cpf
# Column mask: returns raw cpf only when acl_consultora has a row matching
# (current_user(), <this cpf>). Same scoping caveat as consultora_rls
# applies — `acl.cpf = cpf` inside a subquery WHERE collapses to
# tautology because both names resolve to the column. IN-form keeps
# the parameter in the outer scope. Mask glyph is the conventional
# Brazilian-CPF format ('***.***.***-**') so on-stage SELECTs render
# unambiguously masked.
create_mask_sql = f"""
CREATE OR REPLACE FUNCTION {target_catalog}.gold.mask_cpf(cpf STRING)
RETURNS STRING
COMMENT 'Column mask for gold.dim_consultora.cpf. Returns raw cpf when acl_consultora has a (current_user(), cpf) grant; otherwise returns ***.***.***-**.'
RETURN CASE
  WHEN cpf IN (
    SELECT acl.cpf
    FROM {target_catalog}.gold.acl_consultora AS acl
    WHERE acl.email = current_user()
  )
  THEN cpf
  ELSE '***.***.***-**'
END
"""

print("Executing CREATE OR REPLACE FUNCTION gold.mask_cpf ...")
spark.sql(create_mask_sql)
print("OK.")

# COMMAND ----------

# DBTITLE 1,Attach row filter + column mask to gold.dim_consultora
# These two ALTER statements require gold.dim_consultora to exist —
# i.e., the SDP pipeline must have run at least once. We tolerate the
# transient TABLE_OR_VIEW_NOT_FOUND on first deploy by raising a clear
# message; the recommended order is `bundle deploy` -> `gold_setup` ->
# SDP pipeline run -> `governance_setup` (this Job). Slice 12's reset
# script enforces that ordering.

dim_qualified = f"{target_catalog}.gold.dim_consultora"

# Probe table existence rather than letting the ALTER fail with a less
# helpful error. spark.catalog.tableExists requires session catalog
# binding; INFORMATION_SCHEMA is more portable across compute kinds.
probe = spark.sql(f"""
  SELECT 1
  FROM {target_catalog}.information_schema.tables
  WHERE table_catalog = '{target_catalog}'
    AND table_schema = 'gold'
    AND table_name = 'dim_consultora'
""").collect()

if not probe:
    raise RuntimeError(
        f"{dim_qualified} does not exist. Run the SDP pipeline "
        "(sdp_main) at least once before running governance_setup. "
        "See src/notebooks/create_governance.py header for the deploy order."
    )

# SET ROW FILTER replaces any existing filter — idempotent re-run.
print(f"Attaching row filter to {dim_qualified} ...")
spark.sql(
    f"ALTER TABLE {dim_qualified} "
    f"SET ROW FILTER {target_catalog}.gold.consultora_rls ON (regiao)"
)
print("OK.")

# SET MASK replaces any existing mask on the column — idempotent.
print(f"Attaching column mask to {dim_qualified}.cpf ...")
spark.sql(
    f"ALTER TABLE {dim_qualified} "
    f"ALTER COLUMN cpf SET MASK {target_catalog}.gold.mask_cpf"
)
print("OK.")

# COMMAND ----------

# DBTITLE 1,Verify
# Smoke verification: UDFs callable, table reachable. Behavioral
# coverage (RLS filter actually filters, mask actually masks for the
# right caller) lives in src/tests/test_rls_mask.py.
print("Smoke checks:")

count = spark.sql(
    f"SELECT count(*) AS n FROM {target_catalog}.gold.acl_consultora"
).collect()[0]["n"]
print(f"  acl_consultora row count: {count}")

mask_no_grant = spark.sql(
    f"SELECT {target_catalog}.gold.mask_cpf('00000000000') AS m"
).collect()[0]["m"]
print(f"  mask_cpf('00000000000') with no grant: {mask_no_grant!r}")
assert mask_no_grant == "***.***.***-**", (
    f"smoke check failed: expected ***.***.***-**, got {mask_no_grant!r}"
)

rls_no_grant = spark.sql(
    f"SELECT {target_catalog}.gold.consultora_rls('Sudeste') AS r"
).collect()[0]["r"]
print(f"  consultora_rls('Sudeste') with no grant: {rls_no_grant}")
# May be True if a grant for current_user() pre-existed; do not assert.

print("Done.")
