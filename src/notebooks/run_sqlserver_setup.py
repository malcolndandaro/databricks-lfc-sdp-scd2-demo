# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server Setup — Slice 02
# MAGIC
# MAGIC One-shot setup for the throwaway SQL Server backing the Direct-Sales E2E demo.
# MAGIC Run as a Databricks Job (`sqlserver_setup` resource); two phases keyed
# MAGIC off the `phase` task parameter:
# MAGIC
# MAGIC | Phase | Scripts run | Purpose |
# MAGIC |-------|-------------|---------|
# MAGIC | `phase_1_seed` | `01_create_database.sql`, `02_create_tables.sql`, `03_seed_data.sql` | Create DB / table / Change Tracking and seed 500 Consultoras with hero #42 at `bronze`. |
# MAGIC | `phase_2_transitions` | `04_apply_tier_transitions.sql` | Apply backdated tier transitions, producing Change Tracking events for Lakeflow Connect (Slice 04) to stream. |
# MAGIC
# MAGIC Uses **`python-tds`** (pure-Python TDS implementation). Tried pymssql
# MAGIC first (FreeTDS segfaults on serverless) and pyodbc (no Microsoft ODBC
# MAGIC drivers available on serverless). python-tds has no native deps, so
# MAGIC it sidesteps both problems.

# COMMAND ----------

# MAGIC %pip install python-tds --quiet
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Read parameters
from pathlib import Path

dbutils.widgets.text("phase", "phase_1_seed")
dbutils.widgets.text("schema", "crm_dev", "SQL Server schema (crm_dev or crm_prod)")
dbutils.widgets.text("sqlserver_host", "")
dbutils.widgets.text("sqlserver_port", "1433")
dbutils.widgets.text("sqlserver_database", "DemoDB")
dbutils.widgets.text("sqlserver_secret_scope", "")
dbutils.widgets.text("sqlserver_secret_user_key", "sqlserver-sa-user")
dbutils.widgets.text("sqlserver_secret_password_key", "sqlserver-sa-password")
dbutils.widgets.text("ddl_workspace_path", "")

phase = dbutils.widgets.get("phase")
schema = dbutils.widgets.get("schema")
host = dbutils.widgets.get("sqlserver_host")
port = int(dbutils.widgets.get("sqlserver_port"))
database = dbutils.widgets.get("sqlserver_database")
secret_scope = dbutils.widgets.get("sqlserver_secret_scope")
secret_user_key = dbutils.widgets.get("sqlserver_secret_user_key")
secret_password_key = dbutils.widgets.get("sqlserver_secret_password_key")
ddl_workspace_path = dbutils.widgets.get("ddl_workspace_path")

if schema not in {"crm_dev", "crm_prod"}:
    raise ValueError(
        f"Invalid schema {schema!r}. Must be 'crm_dev' or 'crm_prod'."
    )

print(f"Phase: {phase}")
print(f"Schema: {schema}")
print(f"Target: {host}:{port} / {database}")
print(f"DDL path: {ddl_workspace_path}")

# COMMAND ----------

# DBTITLE 1,Resolve credentials from workspace secrets
sa_user = dbutils.secrets.get(scope=secret_scope, key=secret_user_key)
sa_password = dbutils.secrets.get(scope=secret_scope, key=secret_password_key)

print(f"User: {sa_user} (password redacted)")

# COMMAND ----------

# DBTITLE 1,Phase → script list
PHASE_TO_FILES = {
    "phase_0_truncate": [
        "00_truncate.sql",
    ],
    "phase_1_seed": [
        "01_create_database.sql",
        "02_create_tables.sql",
        "03_seed_data.sql",
    ],
    "phase_2_transitions": [
        "04_apply_tier_transitions.sql",
    ],
}

if phase not in PHASE_TO_FILES:
    raise ValueError(
        f"Unknown phase {phase!r}. Expected one of {list(PHASE_TO_FILES)}."
    )

scripts = PHASE_TO_FILES[phase]
print(f"Will run {len(scripts)} script(s):")
for s in scripts:
    print(f"  - {s}")

# COMMAND ----------

# DBTITLE 1,Read SQL file contents
def read_sql_file(name: str) -> str:
    path = Path(ddl_workspace_path) / name
    if not path.exists():
        raise FileNotFoundError(
            f"DDL file not found at {path}. "
            f"Ensure `databricks bundle deploy -t <target>` ran successfully."
        )
    return path.read_text(encoding="utf-8")


sql_payloads = {
    name: read_sql_file(name).replace("{{schema}}", schema)
    for name in scripts
}
for name, payload in sql_payloads.items():
    n_lines = payload.count("\n")
    n_bytes = len(payload.encode("utf-8"))
    print(f"  {name}: {n_lines} lines, {n_bytes} bytes")

# COMMAND ----------

# DBTITLE 1,Connect + execute
import pytds  # type: ignore[import-not-found]


def split_on_go(sql: str) -> list[str]:
    """Split a T-SQL script on standalone `GO` separators (case-insensitive,
    must be on its own line). pytds does not understand `GO`; it's a sqlcmd
    directive. We split client-side and send each batch as one execute call.
    """
    batches: list[str] = []
    current: list[str] = []
    for line in sql.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            current = []
        else:
            current.append(line)
    tail = "\n".join(current).strip()
    if tail:
        batches.append(tail)
    return batches


def execute_batch(cursor, batch: str) -> None:
    """Execute one T-SQL batch.

    For batches that consist of many ``IF NOT EXISTS / INSERT`` pairs
    (i.e. the 500-row seed file), pytds was observed to return success
    after only ~175 statements when the entire batch was sent as one
    ``execute()`` call. To make the seed reliable, we detect that shape
    and execute each ``IF NOT EXISTS / INSERT`` pair individually.

    Heuristic: more than 50 INSERT statements in the batch → split on
    ``;`` boundaries and execute each non-empty statement separately.
    Otherwise execute the whole batch as one (preserves multi-statement
    control flow like ``IF / BEGIN / END`` blocks in the DDL files).
    """
    insert_count = batch.count(f"INSERT INTO {schema}.consultoras")
    if insert_count > 50:
        # Split on `;` end-of-statement markers. Re-attach the IF guard to
        # its INSERT by joining lines until we see a closing `;`.
        statements: list[str] = []
        buf: list[str] = []
        for line in batch.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue
            buf.append(line)
            if stripped.endswith(";"):
                stmt = "\n".join(buf).strip().rstrip(";")
                if stmt:
                    statements.append(stmt)
                buf = []
        if buf:
            tail = "\n".join(buf).strip().rstrip(";")
            if tail:
                statements.append(tail)
        for stmt in statements:
            cursor.execute(stmt)
    else:
        cursor.execute(batch)


# Connect to master initially. 01_create_database.sql may issue its own
# `USE DemoDB`. Subsequent scripts also `USE DemoDB` at the top.
conn = pytds.connect(
    server=host,
    port=port,
    user=sa_user,
    password=sa_password,
    database="master",
    autocommit=True,
    login_timeout=30,
    timeout=120,
)
print(f"Connected to {host}:{port} (DB=master)")

try:
    for name, payload in sql_payloads.items():
        print(f"\n=== Running {name} ===")
        batches = split_on_go(payload)
        cursor = conn.cursor()
        for i, batch in enumerate(batches, start=1):
            execute_batch(cursor, batch)
            # python-tds exposes server PRINT messages on the connection.
            for msg in (getattr(conn, "messages", []) or []):
                if isinstance(msg, tuple) and len(msg) >= 2 and msg[1]:
                    print(f"  [SERVER] {msg[1]}")
            if hasattr(conn, "messages"):
                conn.messages.clear()
            print(
                f"  batch {i}/{len(batches)} ok "
                f"({batch.count(chr(10)) + 1} lines)"
            )
        cursor.close()
finally:
    conn.close()

print("\nAll scripts completed.")

# COMMAND ----------

# DBTITLE 1,Verification (phase-specific)
verification_queries = {
    "phase_0_truncate": [
        (
            "Total Consultoras after TRUNCATE (expect 0)",
            f"SELECT COUNT(*) AS n FROM {schema}.consultoras",
        ),
    ],
    "phase_1_seed": [
        ("Total Consultoras (expect 500)", f"SELECT COUNT(*) AS n FROM {schema}.consultoras"),
        (
            "Tier distribution (expect 200/150/90/50/10 across 5 tiers; "
            "hero #42 forced to bronze)",
            (
                f"SELECT tier, COUNT(*) AS n FROM {schema}.consultoras "
                "GROUP BY tier ORDER BY n DESC"
            ),
        ),
        (
            "Hero #42 (expect tier=bronze, data_cadastro=2024-01-15)",
            (
                f"SELECT consultora_id, tier, data_cadastro, updated_at "
                f"FROM {schema}.consultoras WHERE consultora_id = 42"
            ),
        ),
        (
            "Malformed CPF (expect exactly 1 row)",
            (
                f"SELECT consultora_id, cpf FROM {schema}.consultoras "
                "WHERE cpf NOT LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'"
            ),
        ),
        (
            "Change Tracking enabled on DemoDB (expect 1 row)",
            (
                "SELECT d.name FROM sys.databases d "
                "JOIN sys.change_tracking_databases ct "
                "ON d.database_id = ct.database_id "
                "WHERE d.name = 'DemoDB'"
            ),
        ),
        (
            f"Change Tracking enabled on {schema}.consultoras (expect 1 row)",
            (
                "SELECT s.name + '.' + t.name AS qualified FROM sys.change_tracking_tables ct "
                "JOIN sys.tables t ON ct.object_id = t.object_id "
                "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                f"WHERE s.name = '{schema}' AND t.name = 'consultoras'"
            ),
        ),
    ],
    "phase_2_transitions": [
        (
            "Hero #42 after transition (expect tier=prata, updated_at=2026-02-01)",
            (
                f"SELECT consultora_id, tier, updated_at FROM {schema}.consultoras "
                "WHERE consultora_id = 42"
            ),
        ),
        (
            "Other transitioned Consultoras (expect 48 rows, updated_at=2025-11-01) — "
            "id%10==0 minus hero #42, minus the 2 seeds already at diamante "
            "(filtered by the script 04 WHERE clause; diamante is the top tier)",
            (
                f"SELECT COUNT(*) AS n FROM {schema}.consultoras "
                "WHERE consultora_id <> 42 "
                "AND consultora_id % 10 = 0 "
                "AND updated_at = '2025-11-01 00:00:00.000'"
            ),
        ),
        (
            "Sample of post-transition rows (10 rows for spot-check)",
            (
                f"SELECT TOP 10 consultora_id, tier, updated_at "
                f"FROM {schema}.consultoras WHERE updated_at = '2025-11-01 00:00:00.000' "
                "ORDER BY consultora_id"
            ),
        ),
        (
            "Change Tracking version (expect a value > 0 — events have been logged)",
            "SELECT CHANGE_TRACKING_CURRENT_VERSION() AS current_version",
        ),
    ],
}

conn = pytds.connect(
    server=host,
    port=port,
    user=sa_user,
    password=sa_password,
    database=database,
    autocommit=True,
    login_timeout=30,
    timeout=60,
)

try:
    for label, query in verification_queries[phase]:
        print(f"\n=== {label} ===")
        cursor = conn.cursor()
        cursor.execute(query)
        cols = [c[0] for c in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        if not rows:
            print("  (no rows)")
        else:
            for r in rows[:25]:
                row_dict = dict(zip(cols, r))
                print(f"  {row_dict}")
            if len(rows) > 25:
                print(f"  ... ({len(rows) - 25} more rows)")
        cursor.close()
finally:
    conn.close()

# COMMAND ----------

# DBTITLE 1,Gateway catch-up wait (phase_2_transitions only)
# The LFC ingestion gateway replicates SQL Server Change Tracking events
# asynchronously from the source. After phase_2_transitions commits 49
# UPDATE rows (hero #42 + 48 others), the gateway needs a window to
# propagate every CT event into its staging volume before the next task
# (`lfc_post_transition_sync`) triggers the ingestion pipeline.
#
# Without this wait, we observed on 2026-05-11 that the gateway propagated
# only 1 of 49 CT events, so bronze.consultoras_raw received only hero's
# update event and the AutoCDC SCD2 in gold.dim_consultora collapsed to a
# single open-prata row instead of the canonical closed-bronze + open-prata
# pair. The wait gives the gateway a deterministic catch-up window.
#
# 180 seconds chosen as belt-and-suspenders insurance after the 2026-05-11
# 1-of-49 incident. Cheap relative to the rest of the reset (5-8 minutes).
import time

if phase == "phase_2_transitions":
    wait_sec = 180
    print(
        f"\nWaiting {wait_sec}s for LFC gateway to replicate {len(rows) if rows else 'all'} "
        f"CT events before lfc_post_transition_sync triggers…"
    )
    time.sleep(wait_sec)
    print("Gateway catch-up window elapsed; continuing to next task.")
