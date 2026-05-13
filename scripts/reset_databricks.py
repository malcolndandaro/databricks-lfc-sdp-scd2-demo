"""Reset the Databricks workspace side of the demo.

This is the longest-running and most-destructive of the three partial-reset
scripts. It drops every schema in ``directsales_dev`` and rebuilds the medallion
end-to-end so the demo starts from the canonical pre-rehearsal state.

Run order (Slice 12 cross-references from Slices 04, 07, 08, 09 all rolled in)::

    1.  Assert --target=dev and prompt for 5-second confirmation (skip with --yes).
    2.  DROP SCHEMA bronze | silver | gold CASCADE on directsales_dev. The
        `_lfc_staging` schema is deliberately preserved — see the long
        comment on SCHEMAS_TO_DROP below for why dropping it breaks the
        LFC gateway permanently.
    3.  databricks bundle deploy -t dev (DATABRICKS_BUNDLE_ENGINE=direct)
        Recreates schemas (catalogs/directsales.yml), pipeline resources
        (LFC gateway/ingestion + sdp_main), and Job resources
        (gold_setup, sqlserver_setup, governance_setup).
    4.  Run gold_setup Job (via _run_job_via_sdk — network-resilient
        SDK polling, NOT the CLI's WebSocket which drops mid-run).
        Creates the comissao_pct UDF. Slice 07 cross-reference: must run
        BEFORE the SDP pipeline because MV2 references the UDF and SDP
        compilation will fail with "function not found" otherwise.
    5.  Re-upload pedidos parquet to bronze.lz volume. Bundle deploy
        recreates the volume empty (new S3 path); without this step SDP
        bronze.pedidos_raw fails with "no such file or directory".
        Delegated to scripts/reset_volume.py.
    6.  Run sqlserver_setup Job (via _run_job_via_sdk). 6 tasks:
        create_uc_connection -> phase_0_truncate -> phase_1_seed ->
        lfc_initial_sync -> phase_2_transitions (+180s gateway catch-up
        wait baked into the notebook) -> lfc_post_transition_sync.

        The 180s wait at the end of phase_2_transitions is load-bearing:
        without it the gateway propagated only 1 of 49 SQL Server CT
        events to bronze.consultoras_raw (2026-05-11 incident), so
        dim_consultora's hero #42 SCD2 history collapsed to a single
        open-prata row instead of the canonical closed-bronze +
        open-prata pair. See run_sqlserver_setup.py for the rationale.
    7.  Trigger sdp_main pipeline with full_refresh=True and wait
        for completion. Recreates silver.* and gold.* (dim_consultora,
        fact_pedido, MV1, MV2). AutoCDC SCD2 reads bronze's CDF stream
        and builds hero #42's closed-bronze + open-prata SCD2 history
        from step 6's two LFC sync events. At this point
        gold.dim_consultora exists but has no row filter or column mask.
    8.  Run governance_setup Job (via _run_job_via_sdk). Recreates
        gold.acl_consultora, gold.consultora_rls, gold.mask_cpf, and
        re-attaches ROW FILTER + cpf MASK to gold.dim_consultora.
    9.  INSERT INTO gold.acl_consultora the baseline admin row + verify:

    NOTE: an earlier iteration ran `_full_refresh_lfc_ingestion` between
    step 6 and the SDP refresh. We dropped it because the full_refresh
    wipes bronze.consultoras_raw and re-snapshots from CURRENT SQL Server
    state, destroying the bronze->prata CDF history that step 6 just
    produced. The helper is preserved in the source as a recovery tool
    for "bronze drifted out of sync" scenarios (not on the happy path).
        - bronze.consultoras_raw row count == 500
        - bronze.pedidos_raw row count == 50000
        - bronze.pedidos_raw schema does NOT include canal_origem
          (Slice 09 cross-reference: rolled-back pre-evolution baseline).
        - gold.fact_pedido > 0 rows; MV2 < fact_pedido (Slice 07
          cross-reference: legitimate gap from Pedidos predating their
          consultora's earliest SCD2 __START_AT).
        - SELECT count(*) FROM gold.dim_consultora AS current_user()
          == 0 (the row filter is in effect for the runner with no grant).

PRD AC 36: every destructive operation asserts ``--target == 'dev'`` and
the script refuses to start if any other target is passed.

Auth: ``WorkspaceClient()`` reads from your active Databricks profile (via
``DATABRICKS_CONFIG_PROFILE`` or the default profile). All workspace state
mutations route through the SDK; SQL Server credentials live in workspace
secrets and are not read locally.

Usage::

    python scripts/reset_databricks.py
    python scripts/reset_databricks.py --yes  # skip prompt
    python scripts/reset_databricks.py --warehouse-id <id>  # explicit warehouse for SQL ops
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from typing import Sequence

DEFAULT_TARGET_CATALOG: str = "directsales_dev"

# Schemas to drop. Order doesn't matter (CASCADE cleans up cross-schema
# refs).
#
# IMPORTANT: do NOT drop `_lfc_staging`. The Lakeflow Connect ingestion
# gateway lazily creates a volume named
# `__databricks_ingestion_gateway_staging_data-<gateway-pipeline-id>`
# inside this schema on its very FIRST run, and never recreates it on
# subsequent runs (verified empirically: full_refresh after schema drop
# fails with INGESTION_GATEWAY_CONNECTION_ERROR.DESTINATION_NOT_AVAILABLE).
#
# We tried Plan B (drop + force gateway full_refresh to recreate volume)
# and it does NOT work — the gateway's volume-creation logic runs only at
# pipeline-resource creation time, not on update. Recovery from a stale
# state requires deleting the gateway pipeline RESOURCE so bundle deploy
# creates a new pipeline with a new id, whose first run creates a fresh
# volume. That's heavier than this reset script wants to be; the
# pragmatic choice is to treat the staging volume as durable
# infrastructure (like the catalog itself) and preserve it across resets.
SCHEMAS_TO_DROP: tuple[str, ...] = (
    "bronze",
    "silver",
    "gold",
)

# Bundle resource names. Keep in sync with resources/jobs/*.yml +
# resources/pipelines/*.yml.
GOLD_SETUP_JOB: str = "gold_setup"
SQLSERVER_SETUP_JOB: str = "sqlserver_setup"
GOVERNANCE_SETUP_JOB: str = "governance_setup"
LFC_GATEWAY_PIPELINE_KEY: str = "lfc_consultoras_gateway"
SDP_MAIN_PIPELINE_KEY: str = "sdp_main"

# Gateway full-refresh: after the schema drop wipes the gateway's
# auto-managed staging volume, we trigger the gateway with full_refresh=true
# so it recreates the volume + reinitializes state. A continuous gateway's
# steady state after init is `RUNNING` (it never COMPLETES). Wait up to 10
# minutes — first-time bootstrap on a freshly-recreated _lfc_staging schema
# can hit gateway compute cold start.
GATEWAY_FULL_REFRESH_TIMEOUT_SEC: int = 600
GATEWAY_FULL_REFRESH_POLL_SEC: int = 15

# SDP pipeline full-refresh — wait up to 15 minutes; serverless usually
# completes in 1-3 minutes, but a cold start with first-time pipeline
# compilation can take longer.
SDP_RUN_TIMEOUT_SEC: int = 900

# LFC ingestion pipeline full-refresh — re-snapshots all source tables to
# bronze. Must be triggered explicitly after the bronze schema drop because
# the ingestion pipeline's CT cursor outlives its destination table; without
# full_refresh, on its next run it only writes the changes-since-last-sync
# (~50 rows from idempotent script 04 transitions) and bronze.consultoras_raw
# ends up with 49 rows instead of 500.
LFC_INGESTION_FULL_REFRESH_TIMEOUT_SEC: int = 600

# Volume path used by Auto Loader to ingest pedidos parquet (Slice 03 design).
# After DROP SCHEMA bronze CASCADE, the volume is gone and bundle deploy
# recreates it empty — we must re-upload the 50 canonical parquet files
# before triggering SDP, otherwise SDP's bronze.pedidos_raw flow fails with
# "no such file or directory" on the underlying S3 path.
VOLUME_PEDIDOS_PATH: str = "/Volumes/{catalog}/bronze/lz/pedidos"
RESET_VOLUME_SCRIPT: str = "scripts/reset_volume.py"

# Outer-layer retry for SDK polling calls. The Databricks SDK already retries
# transient errors internally before raising TimeoutError / ConnectionError;
# this wrapper handles the case where the SDK gave up. Real-world: a single
# RemoteDisconnected during a 10-minute pipeline poll should not kill the
# entire reset.
SDK_POLL_RETRY_ATTEMPTS: int = 6
SDK_POLL_RETRY_SLEEP_SEC: int = 15

# Baseline admin row inserted into gold.acl_consultora. The email is a
# placeholder so it does not match anyone's current_user() and the demo's
# "Malcoln sees nothing" starting state is preserved (PRD AC 41).
ADMIN_BASELINE_EMAIL: str = "admin@demo.invalid"
ADMIN_BASELINE_REGIAO: str = "Sudeste"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reset the Databricks side of the demo: DROP SCHEMA cascade, "
            "bundle deploy, gold_setup, sqlserver_setup (with LFC syncs), "
            "SDP full_refresh, governance_setup, and acl_consultora baseline."
        )
    )
    parser.add_argument(
        "--target",
        default="dev",
        choices=["dev"],
        help=(
            "Bundle target. Locked to 'dev' — never touches directsales_prod "
            "(PRD AC 36)."
        ),
    )
    parser.add_argument(
        "--target-catalog",
        default=DEFAULT_TARGET_CATALOG,
        help=f"Unity Catalog name (default: {DEFAULT_TARGET_CATALOG}).",
    )
    parser.add_argument(
        "--warehouse-id",
        default=None,
        help=(
            "SQL warehouse id used for the DROP SCHEMA / INSERT / "
            "verification SQL. If omitted, the first RUNNING warehouse "
            "in the workspace is used."
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help=(
            "Skip the 5-second confirmation prompt before DROP SCHEMA. "
            "The Makefile's `reset-databricks` target passes this."
        ),
    )
    parser.add_argument(
        "--skip-prewarm",
        action="store_true",
        help=(
            "Skip the LFC gateway pre-warm wait. Useful in nested resets "
            "where you've already verified the gateway is hot."
        ),
    )
    return parser.parse_args(argv)


def _assert_target_dev(target: str, target_catalog: str) -> None:
    """PRD AC 36: every destructive demo-reset op asserts target == 'dev'.

    Defense in depth — argparse already restricts ``--target`` and we cross-
    check the catalog name is the dev catalog (a typo'd ``--target-catalog``
    would otherwise slip through).
    """
    if target != "dev":
        raise SystemExit(
            f"refusing to reset Databricks with target={target!r}; "
            "this script is hardcoded to target='dev' (PRD AC 36)."
        )
    if target_catalog == "directsales_prod":
        raise SystemExit(
            f"refusing to reset target_catalog={target_catalog!r}; "
            "this script must never run against directsales_prod (PRD AC 36)."
        )


def _confirm(yes: bool, target_catalog: str) -> None:
    """5-second confirmation prompt before DROP SCHEMA."""
    if yes:
        return
    print(
        f"About to DROP SCHEMA CASCADE on {target_catalog}.{{"
        + ",".join(SCHEMAS_TO_DROP)
        + "}} and rebuild end-to-end."
    )
    print(
        "Reset takes ~3 minutes when warm; up to ~8 minutes on first run "
        "after a fresh workspace deploy (gateway cold start)."
    )
    print("Press Ctrl-C in the next 5 seconds to abort.")
    for i in range(5, 0, -1):
        print(f"  {i}…", flush=True)
        time.sleep(1)


def _resolve_warehouse_id(client, override: str | None) -> str:
    """Pick a warehouse id for SQL execution, with a clear error if none.

    Auto-discovery preference order:
      1. ``--warehouse-id`` if passed (explicit always wins).
      2. The first RUNNING warehouse in the workspace.
      3. Otherwise: fail with an explicit error pointing the user at
         ``databricks warehouses list`` and ``--warehouse-id``.
    """
    if override:
        return override
    from databricks.sdk.service.sql import State

    for w in client.warehouses.list():
        if w.state == State.RUNNING:
            return w.id
    raise SystemExit(
        "no RUNNING SQL warehouses found in the workspace. Start one in "
        "the UI (or with `databricks warehouses start <id>`), then re-run "
        "with --warehouse-id <id>."
    )


def _execute_sql(client, warehouse_id: str, sql: str) -> list[list]:
    """Execute a single SQL statement on the given warehouse.

    Returns a list of result rows (each row is a list of column values).
    Raises if the statement fails. Uses the synchronous statement-execution
    API with a 60-second wait_timeout — DROP SCHEMA CASCADE on the demo
    workspace is consistently sub-second.
    """
    from databricks.sdk.service.sql import StatementState

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        response = client.statement_execution.get_statement(
            statement_id=response.statement_id
        )
    if response.status.state != StatementState.SUCCEEDED:
        raise SystemExit(
            f"SQL statement failed (state={response.status.state!r}): "
            f"{response.status.error.message if response.status.error else 'no error message'}\n"
            f"  SQL: {sql}"
        )
    if response.result and response.result.data_array:
        return list(response.result.data_array)
    return []


def _drop_schemas(client, warehouse_id: str, target_catalog: str) -> None:
    """Drop the four medallion schemas + the LFC staging schema.

    Each statement is ``DROP SCHEMA IF EXISTS`` so missing schemas are not
    fatal (e.g. on a workspace where ``_lfc_staging`` was never created or
    has already been dropped). CASCADE removes dependent tables/views/UDFs.

    Note that the UC connection (``directsales_sqlserver_conn``) is **not** in
    this list — Slice 04 cross-reference: bundle deploy validates it exists
    eagerly; dropping it bricks the next deploy.
    """
    for schema in SCHEMAS_TO_DROP:
        sql = f"DROP SCHEMA IF EXISTS {target_catalog}.{schema} CASCADE"
        print(f"  {sql}")
        _execute_sql(client, warehouse_id, sql)


def _bundle_run(target: str, resource: str) -> None:
    """Run a bundle resource via ``databricks bundle run``.

    Synchronous: blocks until the Job/pipeline completes. Streams CLI output
    to stdout/stderr so failures surface with the underlying task name.
    """
    cmd = ["databricks", "bundle", "run", resource, "-t", target]
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise SystemExit(
            f"`databricks bundle run {resource} -t {target}` failed with "
            f"exit code {result.returncode}. Inspect the resource in the "
            "workspace UI and re-run reset_databricks.py from the failed "
            "step."
        )


def _bundle_deploy(target: str) -> None:
    """Run ``databricks bundle deploy -t <target>`` synchronously.

    Sets ``DATABRICKS_BUNDLE_ENGINE=direct`` per Slice 01: catalog resources
    require the direct deployment engine. Without it, deploys that touch
    catalog declarations (or run on a workspace where the catalog has been
    dropped) fail with "Catalog resources are only supported with direct
    deployment mode." Setting it always is safe; non-catalog resources
    deploy identically under direct.
    """
    cmd = ["databricks", "bundle", "deploy", "-t", target]
    print(f"$ {' '.join(cmd)} (DATABRICKS_BUNDLE_ENGINE=direct)")
    env = {**os.environ, "DATABRICKS_BUNDLE_ENGINE": "direct"}
    result = subprocess.run(cmd, check=False, env=env)
    if result.returncode != 0:
        raise SystemExit(
            f"`databricks bundle deploy -t {target}` failed with exit code "
            f"{result.returncode}. Inspect the CLI output and fix before "
            "re-running."
        )


def _find_pipeline_id(
    client, name_substring: str, target_catalog: str, target: str = "dev"
) -> str:
    """Look up a pipeline id by partial name match, scoped to the target.

    All bundle resources are now target-prefixed (Slice 13) as
    ``[{target}] …`` so prod + dev can coexist in one workspace. The
    lookup matches BOTH the target prefix AND a name substring, so
    ``substring='sdp-main', target='dev'`` only resolves to
    ``[dev] sdp-main`` even when ``[prod] sdp-main``
    exists alongside.
    """
    target_prefix = f"[{target}]"
    candidates = []
    for p in client.pipelines.list_pipelines():
        name = p.name or ""
        if name.startswith(target_prefix) and name_substring in name:
            candidates.append(p)
    if not candidates:
        raise SystemExit(
            f"no pipeline found matching name substring "
            f"{name_substring!r} with prefix {target_prefix!r} in "
            f"catalog={target_catalog!r}. Did `databricks bundle deploy "
            f"-t {target}` succeed?"
        )
    if len(candidates) > 1:
        names = [p.name for p in candidates]
        raise SystemExit(
            f"multiple pipelines match {name_substring!r} under "
            f"{target_prefix}: {names}. Refine the substring."
        )
    return candidates[0].pipeline_id


def _safe_call(fn, *args, max_attempts: int = SDK_POLL_RETRY_ATTEMPTS,
               sleep_sec: int = SDK_POLL_RETRY_SLEEP_SEC, **kwargs):
    """Call ``fn(*args, **kwargs)``; retry on transient network errors.

    The Databricks SDK already retries internally on transient HTTP errors
    before raising ``TimeoutError`` / ``ConnectionError``. This helper is the
    OUTER retry: if the SDK gave up, sleep and try again. Catches all
    exceptions (broad on purpose — pipeline-polling loops should not die on
    any single fetch failure during a 10-minute reset).

    Re-raises the last exception if all attempts fail.
    """
    last_err: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_err = exc
            print(
                f"  [retry] {type(exc).__name__}: {str(exc)[:120]} "
                f"(attempt {attempt + 1}/{max_attempts})"
            )
            if attempt + 1 < max_attempts:
                time.sleep(sleep_sec)
    assert last_err is not None
    raise last_err


def _stop_pipeline_if_active(
    client,
    pipeline_id: str,
    *,
    timeout_sec: int = 120,
    poll_sec: int = 5,
) -> None:
    """Stop the pipeline if it has an in-flight update; wait for IDLE.

    A continuous pipeline (the LFC gateway) always has an active update —
    its steady state is ``RUNNING``. To trigger a new ``full_refresh`` we
    must stop the current update first; otherwise ``start_update`` fails
    with ``ResourceConflict: An active update '...' already exists``.

    Idempotent: if the pipeline is already IDLE / FAILED / STOPPED, this
    is a no-op.
    """
    from databricks.sdk.service.pipelines import PipelineState

    pipeline = _safe_call(client.pipelines.get, pipeline_id)
    state = pipeline.state
    print(f"  current pipeline state: {state}")

    # Terminal / non-running states — nothing to stop.
    quiescent = {
        PipelineState.IDLE,
        PipelineState.FAILED,
    }
    # Some SDK versions also expose DELETED; tolerate its absence.
    if hasattr(PipelineState, "DELETED"):
        quiescent.add(PipelineState.DELETED)
    if state in quiescent:
        return

    print(f"  stopping pipeline (state={state!r}) before full_refresh…")
    _safe_call(client.pipelines.stop, pipeline_id=pipeline_id)

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        pipeline = _safe_call(client.pipelines.get, pipeline_id)
        if pipeline.state in quiescent:
            print(f"  pipeline now {pipeline.state!r}; safe to start_update.")
            return
        print(f"  waiting for pipeline to stop (state={pipeline.state!r})…")
        time.sleep(poll_sec)
    raise SystemExit(
        f"Pipeline {pipeline_id} did not reach IDLE within {timeout_sec}s "
        "after stop request. Inspect the workspace UI."
    )


def _full_refresh_gateway(
    client,
    *,
    target_catalog: str,
    timeout_sec: int = GATEWAY_FULL_REFRESH_TIMEOUT_SEC,
    poll_sec: int = GATEWAY_FULL_REFRESH_POLL_SEC,
) -> None:
    """Trigger the LFC gateway with full_refresh=True and wait for RUNNING.

    The gateway is a *continuous* pipeline, so its steady state after init
    is ``RUNNING`` — it never reaches ``COMPLETED``. We treat the first
    transition into ``RUNNING`` after the full-refresh trigger as success.

    Order of operations:

    1. ``_stop_pipeline_if_active`` — a continuous gateway always has an
       in-flight update; we must stop it before triggering a new update,
       or ``start_update`` fails with ResourceConflict.
    2. ``start_update(full_refresh=True)`` — recreates the gateway's
       auto-managed staging volume
       (``__databricks_ingestion_gateway_staging_data-<pipeline_id>``) that
       step [1/9]'s schema drop wiped. Without this, the gateway does not
       self-heal and ingestion fails with ``UC_VOLUME_NOT_FOUND``.
    3. Poll until the new update reaches RUNNING.
    """
    from databricks.sdk.service.pipelines import UpdateInfoState

    pipeline_id = _find_pipeline_id(
        client, "LFC SQL Server gateway", target_catalog
    )
    print(
        f"Triggering gateway pipeline {pipeline_id} with full_refresh=true "
        f"(timeout {timeout_sec}s)…"
    )

    # Stop any in-flight update before starting a new one.
    _stop_pipeline_if_active(client, pipeline_id)

    update = _safe_call(client.pipelines.start_update, 
        pipeline_id=pipeline_id, full_refresh=True
    )
    update_id = update.update_id
    print(f"  gateway update_id: {update_id}")

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        info = _safe_call(client.pipelines.get_update, 
            pipeline_id=pipeline_id, update_id=update_id
        )
        state = info.update.state if info.update else None
        if state == UpdateInfoState.RUNNING:
            print(
                f"  gateway entered RUNNING — staging volume recreated, "
                f"ingestion can proceed."
            )
            return
        if state in (UpdateInfoState.FAILED, UpdateInfoState.CANCELED):
            raise SystemExit(
                f"Gateway full_refresh ended in state {state!r}. Inspect "
                f"events at workspace -> pipelines -> {pipeline_id} -> "
                f"{update_id}."
            )
        print(f"  gateway state={state!r}; waiting {poll_sec}s…")
        time.sleep(poll_sec)
    raise SystemExit(
        f"Gateway full_refresh did not reach RUNNING within {timeout_sec}s "
        f"(update {update_id}). Inspect events for the gateway pipeline "
        f"in the workspace UI before re-running."
    )


def _repopulate_pedidos_volume(target_catalog: str) -> None:
    """Re-upload the 50 canonical pedidos parquet files into the bronze volume.

    The bronze schema drop in step [1/9] wipes the bronze.lz volume and all
    its files. Bundle deploy recreates the volume but with a new underlying
    S3 path; the volume is empty. Auto Loader's STREAM read_files() in
    src/sdp/00_bronze.sql then fails ("No such file or directory") unless
    we re-upload the parquet files before triggering SDP.

    Delegated to scripts/reset_volume.py (already proven + has its own
    NotFound handling for the "subfolder doesn't exist yet" case after
    bundle deploy recreates the volume).
    """
    cmd = [sys.executable, RESET_VOLUME_SCRIPT, "--yes",
           "--target-catalog", target_catalog]
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise SystemExit(
            f"`{RESET_VOLUME_SCRIPT}` failed with exit code "
            f"{result.returncode}. Inspect the output above; SDP refresh "
            "will fail without the parquet files in place."
        )


def _ensure_gateway_running(
    client,
    *,
    target_catalog: str,
    target: str = "dev",
    timeout_sec: int = GATEWAY_FULL_REFRESH_TIMEOUT_SEC,
    poll_sec: int = GATEWAY_FULL_REFRESH_POLL_SEC,
) -> None:
    """Ensure the LFC ingestion gateway is in RUNNING state.

    Continuous pipelines are NOT auto-restarted by ``bundle deploy`` or
    ``bundle run`` — if someone stopped the gateway between rehearsals
    (manually, via the workspace UI, or because the workspace paused
    inactive resources), every subsequent ingestion run silently produces
    empty bronze tables. The ingestion update reports SUCCESS but its
    snapshot_flow and cdc_flow are marked EXCLUDED because the gateway
    is not producing data.

    Lightweight: if the gateway is already RUNNING this is a no-op. If
    it's in any other state, trigger a normal ``start_update`` (NOT a
    full_refresh — that's a heavier operation handled elsewhere) and
    wait for RUNNING.

    Hit in practice 2026-05-11: external user stopped the gateway over
    the weekend; the reset script ran end-to-end but bronze.consultoras_raw
    came out empty.
    """
    from databricks.sdk.service.pipelines import PipelineState, UpdateInfoState

    pipeline_id = _find_pipeline_id(
        client, "LFC SQL Server gateway", target_catalog, target=target
    )

    pipeline = _safe_call(client.pipelines.get, pipeline_id)
    state = pipeline.state
    print(f"  gateway current state: {state}")

    if state == PipelineState.RUNNING:
        print("  gateway already RUNNING — no action needed.")
        return

    print(
        f"  gateway is {state!r} (not RUNNING); starting it before ingestion…"
    )
    update = _safe_call(
        client.pipelines.start_update, pipeline_id=pipeline_id, full_refresh=False
    )
    update_id = update.update_id
    print(f"  gateway update_id: {update_id}")

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        info = _safe_call(
            client.pipelines.get_update, pipeline_id=pipeline_id, update_id=update_id
        )
        state = info.update.state if info.update else None
        if state == UpdateInfoState.RUNNING:
            # Brief settle so the gateway has time to start replicating
            # before downstream ingestion picks it up.
            print(
                "  gateway entered RUNNING — sleeping 20s to let it begin replication…"
            )
            time.sleep(20)
            return
        if state in (UpdateInfoState.FAILED, UpdateInfoState.CANCELED):
            raise SystemExit(
                f"Gateway start failed (state={state!r}, update={update_id}). "
                "Inspect the workspace UI."
            )
        print(f"  gateway state={state!r}; waiting {poll_sec}s…")
        time.sleep(poll_sec)
    raise SystemExit(
        f"Gateway did not reach RUNNING within {timeout_sec}s (update "
        f"{update_id}). Inspect the workspace UI before re-running."
    )


def _full_refresh_lfc_ingestion(
    client,
    *,
    target_catalog: str,
    timeout_sec: int = LFC_INGESTION_FULL_REFRESH_TIMEOUT_SEC,
) -> None:
    """Trigger the LFC ingestion pipeline with full_refresh=True.

    Required after bronze schema drop: the ingestion pipeline's Change
    Tracking cursor persists across destination drops, so on its next
    run it only writes change-deltas (the ~50 rows touched by script 04)
    instead of all 500. Force a full re-snapshot to repopulate completely.
    """
    from databricks.sdk.service.pipelines import UpdateInfoState

    pipeline_id = _find_pipeline_id(
        client, "LFC SQL Server ingest", target_catalog
    )
    print(
        f"Triggering ingestion pipeline {pipeline_id} with full_refresh=true "
        f"(timeout {timeout_sec}s) to re-snapshot all 500 Consultoras…"
    )
    _stop_pipeline_if_active(client, pipeline_id)
    update = _safe_call(client.pipelines.start_update, 
        pipeline_id=pipeline_id, full_refresh=True
    )
    update_id = update.update_id

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        info = _safe_call(client.pipelines.get_update, 
            pipeline_id=pipeline_id, update_id=update_id
        )
        state = info.update.state if info.update else None
        if state == UpdateInfoState.COMPLETED:
            print(f"  ingestion update {update_id} completed.")
            return
        if state in (UpdateInfoState.FAILED, UpdateInfoState.CANCELED):
            raise SystemExit(
                f"Ingestion full_refresh ended in state {state!r} "
                f"(update {update_id})."
            )
        print(f"  ingestion state={state!r}; waiting 15s…")
        time.sleep(15)
    raise SystemExit(
        f"Ingestion full_refresh did not complete within {timeout_sec}s "
        f"(update {update_id})."
    )


def _run_job_via_sdk(client, job_name_substring: str, target: str) -> None:
    """Trigger a Job by name and poll for completion via SDK.

    Avoids the failure mode where ``databricks bundle run`` loses its
    WebSocket connection mid-run (transient network blip) and reports
    failure even though the Job actually succeeds on the workspace.
    All Job orchestration in reset goes through this helper.

    Matching is scoped to ``[{target}]``-prefixed Jobs so prod + dev
    Jobs deployed to the same workspace (Slice 13) don't collide on
    the substring match. Reset is dev-only (PRD AC 36) so ``target``
    is always ``'dev'`` here in practice.
    """
    target_prefix = f"[{target}]"
    matching = [
        j for j in client.jobs.list()
        if (j.settings.name or "").startswith(target_prefix)
        and job_name_substring in (j.settings.name or "")
    ]
    if not matching:
        raise SystemExit(
            f"no Job found whose name starts with {target_prefix!r} and "
            f"contains {job_name_substring!r}. Did `databricks bundle "
            f"deploy -t {target}` succeed?"
        )
    if len(matching) > 1:
        names = [j.settings.name for j in matching]
        raise SystemExit(
            f"multiple Jobs match {job_name_substring!r} under "
            f"{target_prefix}: {names}"
        )
    job = matching[0]
    print(f"Triggering Job '{job.settings.name}' (job_id={job.job_id})…")
    run = client.jobs.run_now(job_id=job.job_id)
    print(f"  run_id={run.run_id}")

    deadline = time.time() + 1800  # 30 minutes max per Job
    while time.time() < deadline:
        try:
            r = client.jobs.get_run(run_id=run.run_id)
        except Exception as exc:  # transient network failure → retry
            print(f"  fetch_run failed ({exc}); retrying in 15s…")
            time.sleep(15)
            continue
        life = r.state.life_cycle_state.value
        res = r.state.result_state.value if r.state.result_state else "N/A"
        summary = ", ".join(
            f"{t.task_key}={(t.state.result_state.value if t.state and t.state.result_state else (t.state.life_cycle_state.value if t.state else '?'))}"
            for t in r.tasks or []
        )
        print(f"  [{time.strftime('%H:%M:%S')}] {life}/{res} | {summary}")
        if life in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            if res != "SUCCESS":
                raise SystemExit(
                    f"Job '{job.settings.name}' run {run.run_id} ended "
                    f"in state {life}/{res}. Inspect the workspace UI."
                )
            print(f"  Job '{job.settings.name}' completed successfully.")
            return
        time.sleep(20)
    raise SystemExit(
        f"Job '{job.settings.name}' run {run.run_id} did not terminate "
        f"in 30 minutes."
    )


def _trigger_sdp_full_refresh(client, *, target_catalog: str) -> None:
    """Trigger sdp_main with full_refresh=True and wait for completion.

    Stops any in-flight update first (defensive: a previous reset that
    crashed mid-SDP-run would leave the pipeline in RUNNING, and the next
    ``start_update`` would fail with ResourceConflict — same root cause as
    the gateway path above).
    """
    pipeline_id = _find_pipeline_id(client, "sdp-main", target_catalog)
    _stop_pipeline_if_active(client, pipeline_id)
    print(f"Triggering SDP pipeline {pipeline_id} with full_refresh=true…")
    update = _safe_call(client.pipelines.start_update, 
        pipeline_id=pipeline_id, full_refresh=True
    )
    update_id = update.update_id

    # Poll for completion. start_update returns immediately; the run is
    # async on the workspace.
    from databricks.sdk.service.pipelines import UpdateInfoState

    deadline = time.time() + SDP_RUN_TIMEOUT_SEC
    while time.time() < deadline:
        info = _safe_call(client.pipelines.get_update, 
            pipeline_id=pipeline_id, update_id=update_id
        )
        state = info.update.state if info.update else None
        if state in (UpdateInfoState.COMPLETED,):
            print(f"  SDP update {update_id} completed.")
            return
        if state in (
            UpdateInfoState.FAILED,
            UpdateInfoState.CANCELED,
        ):
            raise SystemExit(
                f"SDP pipeline update {update_id} ended in state {state!r}. "
                f"Inspect events at "
                f"workspace -> pipelines -> {pipeline_id} -> {update_id}."
            )
        print(f"  SDP update {update_id} state={state!r}; waiting 15s…")
        time.sleep(15)
    raise SystemExit(
        f"SDP update {update_id} did not complete in {SDP_RUN_TIMEOUT_SEC}s."
    )


def _seed_acl_consultora_admin(
    client, warehouse_id: str, target_catalog: str
) -> None:
    """Insert the single admin baseline row into gold.acl_consultora.

    PRD AC 41: reset must produce exactly one admin row and zero rows for
    current_user(). The admin email is a placeholder
    (``admin@demo.invalid``) so it does not match any real workspace
    user — keeps the demo's "Malcoln sees nothing" starting state.

    The governance_setup notebook uses ``CREATE TABLE IF NOT EXISTS`` and
    deliberately does NOT touch row contents (Slice 08 design). The reset
    script owns row contents, so this script needs to wipe and re-seed.
    """
    sql = f"DELETE FROM {target_catalog}.gold.acl_consultora"
    print(f"  {sql}")
    _execute_sql(client, warehouse_id, sql)
    sql = (
        f"INSERT INTO {target_catalog}.gold.acl_consultora (email, regiao, cpf) "
        f"VALUES ('{ADMIN_BASELINE_EMAIL}', '{ADMIN_BASELINE_REGIAO}', NULL)"
    )
    print(f"  {sql}")
    _execute_sql(client, warehouse_id, sql)


def _verify_post_reset(
    client, warehouse_id: str, target_catalog: str
) -> None:
    """Sanity-check the workspace returned to the canonical baseline.

    Five queries:
      a. bronze.consultoras_raw row count == 500
      b. bronze.pedidos_raw row count == 50000
      c. bronze.pedidos_raw schema does NOT include canal_origem
         (Slice 09 cross-reference: rolled-back pre-evolution baseline).
      d. gold.fact_pedido > 0 and 0 < MV2 < fact_pedido (Slice 07
         cross-reference: legitimate gap; not equality).
      e. SELECT count(*) FROM gold.dim_consultora == 0 (the row filter is
         in effect for the runner with no acl_consultora grant; Slice 08
         cross-reference).
    """
    failures: list[str] = []

    rows = _execute_sql(
        client,
        warehouse_id,
        f"SELECT COUNT(*) FROM {target_catalog}.bronze.consultoras_raw",
    )
    n_consultoras = int(rows[0][0]) if rows else 0
    if n_consultoras != 500:
        failures.append(
            f"bronze.consultoras_raw count={n_consultoras}, expected 500"
        )

    rows = _execute_sql(
        client,
        warehouse_id,
        f"SELECT COUNT(*) FROM {target_catalog}.bronze.pedidos_raw",
    )
    n_pedidos = int(rows[0][0]) if rows else 0
    if n_pedidos != 50000:
        failures.append(
            f"bronze.pedidos_raw count={n_pedidos}, expected 50000"
        )

    rows = _execute_sql(
        client,
        warehouse_id,
        f"DESCRIBE TABLE {target_catalog}.bronze.pedidos_raw",
    )
    column_names = {str(r[0]) for r in rows if r and r[0]}
    if "canal_origem" in column_names:
        failures.append(
            "bronze.pedidos_raw still has canal_origem column — Slice 09 "
            "schema evolution did not roll back. Did make reset-volume run "
            "before reset-databricks?"
        )

    rows = _execute_sql(
        client,
        warehouse_id,
        f"SELECT COUNT(*) FROM {target_catalog}.gold.fact_pedido",
    )
    n_fact = int(rows[0][0]) if rows else 0
    if n_fact == 0:
        failures.append("gold.fact_pedido is empty")

    rows = _execute_sql(
        client,
        warehouse_id,
        f"SELECT COUNT(*) FROM {target_catalog}.gold.report_comissao_pedido_tier_historico",
    )
    n_mv2 = int(rows[0][0]) if rows else 0
    if n_mv2 == 0:
        failures.append("gold.report_comissao_pedido_tier_historico is empty")
    elif n_mv2 >= n_fact:
        # Slice 07 cross-reference: MV2 must be strictly less than fact_pedido.
        failures.append(
            f"MV2 count ({n_mv2}) is not < fact_pedido ({n_fact}). "
            "Some Pedidos predate their Consultora's earliest SCD2 "
            "__START_AT, so MV2 is expected to be strictly smaller."
        )

    rows = _execute_sql(
        client,
        warehouse_id,
        f"SELECT COUNT(*) FROM {target_catalog}.gold.dim_consultora",
    )
    n_dim_visible = int(rows[0][0]) if rows else 0
    if n_dim_visible != 0:
        failures.append(
            f"gold.dim_consultora visible row count for current_user()"
            f"={n_dim_visible}, expected 0. Either the row filter did not "
            "attach (governance_setup failure) or acl_consultora already has a "
            "current_user() grant from a prior session."
        )

    # SCD2 hero history — the demo's whole point. Bypass row filter by
    # joining with EXISTS against acl_consultora (admin baseline can see all),
    # OR query underlying parquet via DESCRIBE HISTORY. Cleanest path:
    # query the table via the privileged metadata view (no rls bypass
    # needed if we count rows for ANY consultora_id=42, which the row
    # filter doesn't fully block — the filter is on regiao).
    #
    # Wait — row filter IS on regiao, so consultora_id=42 should be
    # visible IFF current_user has acl_consultora row for hero's region.
    # On a fresh reset, baseline acl_consultora has only the admin row;
    # current_user has none. So query as ANY user with admin baseline
    # grant... actually simpler: query the audit-bypassing system table.
    #
    # Pragmatic: temporarily INSERT current_user grant, query, then
    # DELETE. This is what the demo's live moment shows anyway.
    _execute_sql(
        client,
        warehouse_id,
        f"INSERT INTO {target_catalog}.gold.acl_consultora (email, regiao, cpf) "
        f"VALUES (current_user(), 'Sudeste', NULL), "
        f"(current_user(), 'Sul', NULL), "
        f"(current_user(), 'Nordeste', NULL), "
        f"(current_user(), 'Centro-Oeste', NULL), "
        f"(current_user(), 'Norte', NULL)",
    )
    try:
        rows = _execute_sql(
            client,
            warehouse_id,
            f"SELECT COUNT(*) FROM {target_catalog}.gold.dim_consultora "
            f"WHERE consultora_id = 42",
        )
        hero_dim_rows = int(rows[0][0]) if rows else 0
        if hero_dim_rows != 2:
            failures.append(
                f"hero #42 has {hero_dim_rows} row(s) in gold.dim_consultora, "
                f"expected 2 (closed bronze + open prata). "
                f"AutoCDC SCD2 didn't see both CDF events from sqlserver_setup's "
                f"two LFC syncs. Check bronze.consultoras_raw's CDF history."
            )
    finally:
        # Always restore baseline state — admin row only, no current_user grant.
        _execute_sql(
            client,
            warehouse_id,
            f"DELETE FROM {target_catalog}.gold.acl_consultora "
            f"WHERE email = current_user()",
        )

    if failures:
        msg = "\n  ".join(failures)
        raise SystemExit(
            "Post-reset verification failed:\n  " + msg
        )

    print(
        "Post-reset verification OK: "
        f"500 consultoras, 50000 pedidos, "
        f"{n_fact} fact rows, {n_mv2} MV2 rows, "
        f"hero #42 has 2 SCD2 rows in dim_consultora, "
        f"dim row filter active (visible count == 0 after baseline restore)."
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    _assert_target_dev(args.target, args.target_catalog)
    _confirm(args.yes, args.target_catalog)

    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    warehouse_id = _resolve_warehouse_id(client, args.warehouse_id)
    print(f"Using SQL warehouse {warehouse_id} for SQL ops.")

    print(f"\n[1/8] DROP SCHEMA CASCADE on {args.target_catalog}.{{...}}")
    _drop_schemas(client, warehouse_id, args.target_catalog)

    print(f"\n[2/8] databricks bundle deploy -t {args.target}")
    _bundle_deploy(args.target)

    print(f"\n[3/8] Run {GOLD_SETUP_JOB} (creates comissao_pct UDF)")
    _run_job_via_sdk(client, "gold-layer UDF setup", args.target)

    print(
        f"\n[4/8] Re-populate volume {VOLUME_PEDIDOS_PATH.format(catalog=args.target_catalog)}"
    )
    _repopulate_pedidos_volume(args.target_catalog)

    print(
        "\n[5/8] Ensure LFC gateway is RUNNING "
        "(continuous pipelines aren't auto-resumed if externally stopped)"
    )
    _ensure_gateway_running(client, target_catalog=args.target_catalog, target=args.target)

    print(
        f"\n[6/8] Run {SQLSERVER_SETUP_JOB} (8 tasks: connection -> truncate -> "
        f"seed -> lfc_initial_sync -> sdp_initial_refresh -> transitions+180s -> "
        f"lfc_post_transition_sync -> sdp_incremental_refresh). The two embedded "
        f"SDP refreshes are what make AutoCDC SCD2 produce hero #42's "
        f"closed-bronze + open-prata pair in dim_consultora; a single refresh "
        f"collapses the bronze->prata transition because silver's row-tracking-aware "
        f"streaming source upserts. See sqlserver_setup.yml comment for the full "
        f"diagnosis."
    )
    _run_job_via_sdk(client, "SQL Server setup + LFC sync", args.target)

    # Standalone SDP full_refresh used to live at [7/9] here, but it caused
    # the exact bug we're trying to fix: a single fused refresh after both
    # LFC syncs collapsed the bronze->prata transition into final state per
    # row. The two SDP refreshes are now baked into the sqlserver_setup Job
    # DAG (tasks 5 and 8) so they bracket phase_2_transitions and give
    # AutoCDC two incremental snapshots to compare.

    print(f"\n[7/8] Run {GOVERNANCE_SETUP_JOB} (acl_consultora, RLS, mask)")
    _run_job_via_sdk(client, "gold-layer governance", args.target)

    print("\n[8/8] Seed gold.acl_consultora admin baseline + verify")
    _seed_acl_consultora_admin(client, warehouse_id, args.target_catalog)
    _verify_post_reset(client, warehouse_id, args.target_catalog)

    print("\nreset_databricks complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
