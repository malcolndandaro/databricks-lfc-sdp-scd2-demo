# Databricks notebook source
# MAGIC %md
# MAGIC # Create UC Connection for Lakeflow Connect — Slice 04
# MAGIC
# MAGIC One-shot, idempotent task that creates the Unity Catalog connection
# MAGIC `directsales_sqlserver_conn` (type SQLSERVER). Run as the **first task** of
# MAGIC the `sqlserver_setup` Job — must complete before the LFC gateway
# MAGIC pipeline runs.
# MAGIC
# MAGIC Credentials are read from the workspace secret scope `directsales-demo`
# MAGIC (provisioned in Slice 02). UC stores the connection's credentials
# MAGIC encrypted at rest; only metastore admins can read them back.
# MAGIC
# MAGIC `CREATE CONNECTION IF NOT EXISTS` makes this safe to re-run. To
# MAGIC rotate credentials, drop and recreate the connection (or `ALTER
# MAGIC CONNECTION ... OWNER TO` then update via DBSQL — out of scope here).

# COMMAND ----------

# DBTITLE 1,Read parameters
dbutils.widgets.text("sqlserver_host", "")
dbutils.widgets.text("sqlserver_port", "1433")
dbutils.widgets.text("sqlserver_database", "DemoDB")
dbutils.widgets.text("sqlserver_secret_scope", "")
dbutils.widgets.text("sqlserver_secret_user_key", "sqlserver-sa-user")
dbutils.widgets.text("sqlserver_secret_password_key", "sqlserver-sa-password")
dbutils.widgets.text("sqlserver_connection_name", "directsales_sqlserver_conn")

host = dbutils.widgets.get("sqlserver_host")
port = int(dbutils.widgets.get("sqlserver_port"))
database = dbutils.widgets.get("sqlserver_database")
secret_scope = dbutils.widgets.get("sqlserver_secret_scope")
secret_user_key = dbutils.widgets.get("sqlserver_secret_user_key")
secret_password_key = dbutils.widgets.get("sqlserver_secret_password_key")
connection_name = dbutils.widgets.get("sqlserver_connection_name")

print(f"Connection: {connection_name}")
print(f"Target:     {host}:{port} (db: {database})")

# COMMAND ----------

# DBTITLE 1,Resolve credentials
sa_user = dbutils.secrets.get(scope=secret_scope, key=secret_user_key)
sa_password = dbutils.secrets.get(scope=secret_scope, key=secret_password_key)
print(f"User: {sa_user} (password redacted)")

# COMMAND ----------

# DBTITLE 1,Create the UC connection (idempotent)
# Escape any single-quotes in user / password to keep the SQL valid even if
# the throwaway password is later rotated to something with apostrophes.
def _esc(value: str) -> str:
    return value.replace("'", "''")


create_sql = f"""
CREATE CONNECTION IF NOT EXISTS {connection_name}
TYPE SQLSERVER
OPTIONS (
  host '{_esc(host)}',
  port '{port}',
  user '{_esc(sa_user)}',
  password '{_esc(sa_password)}',
  trustServerCertificate 'true'
)
COMMENT 'Throwaway demo connection for Slice 04 LFC ingestion. See sql_server/INSTANCE.md.'
"""

print("Executing CREATE CONNECTION ...")
spark.sql(create_sql)
print("OK.")

# COMMAND ----------

# DBTITLE 1,Verify the connection
desc = spark.sql(f"DESCRIBE CONNECTION EXTENDED {connection_name}").collect()
for row in desc:
    rd = row.asDict()
    # Mask any secret-shaped fields just in case DESCRIBE EXTENDED ever
    # surfaces credentials (it shouldn't — UC redacts these).
    if "password" in str(rd).lower() and "[REDACTED]" not in str(rd):
        rd = {k: ("***" if "password" in k.lower() else v) for k, v in rd.items()}
    print(rd)
