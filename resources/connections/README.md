# UC Connections

This directory is reserved for any future Databricks Asset Bundle `connections` resources. The single connection used by the demo today — **`directsales_sqlserver_conn`** (type `SQLSERVER`) — is **not** declared as a bundle resource, by design.

## Why the connection isn't in the bundle

DABs validates `pipelines.gateway_definition.connection_name` at deploy time and rejects deployment if the named connection doesn't exist. That creates a chicken-and-egg with `bundle deploy`: the bundle wants the connection, but the bundle is what could create it.

**Workaround:** the connection lives outside the bundle and is created/refreshed by:

- **First-time creation**: a one-shot SDK call documented at the bottom of this file.
- **Subsequent resets**: the `create_uc_connection` notebook task at the top of the `sqlserver_setup` Job (which runs `CREATE CONNECTION IF NOT EXISTS` against a SQL warehouse — idempotent).

The connection is referenced by name from `resources/pipelines/lfc_consultoras.yml` via `${var.sqlserver_connection_name}` (default `directsales_sqlserver_conn`).

## Workspace secret scope

Credentials live in the workspace secret scope **`directsales-demo`** with two keys:

- `sqlserver-sa-user` (typically `SA`)
- `sqlserver-sa-password`

The notebook `src/notebooks/create_uc_connection.py` reads both via `dbutils.secrets.get()`. The connection itself stores credentials encrypted in UC; only metastore admins can read them.

If the SA password rotates, update the secret AND drop+recreate the connection (UC connections do not pull from the secret scope at query time).

## First-time bootstrap

If the connection does not exist yet (e.g. a fresh workspace), run this once before the first `databricks bundle deploy`:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

w = WorkspaceClient()
w.connections.create(
    name="directsales_sqlserver_conn",
    connection_type=ConnectionType.SQLSERVER,
    options={
        "host": "<SQLSERVER_HOST>",            # see sql_server/INSTANCE.md
        "port": "1433",
        "user": "SA",
        "password": "<YOUR_SQLSERVER_SA_PASSWORD>",
        "trustServerCertificate": "true",  # camelCase — the snake_case form is rejected
    },
    comment="Throwaway demo connection for Slice 04 LFC ingestion. See sql_server/INSTANCE.md.",
)
```

After that, `databricks bundle deploy -t dev` finds the connection and the gateway/ingestion pipelines deploy cleanly.

## Why not a bundle resource

This is a demo-environment workaround — UC connections are workspace-level and authoritative ownership belongs at the workspace, not in a per-environment bundle. Declaring the connection as a bundle resource would have the bundle reset its credentials on every `deploy`, which is not what we want for a shared SQL Server backing both `dev` and `prod` catalogs.
