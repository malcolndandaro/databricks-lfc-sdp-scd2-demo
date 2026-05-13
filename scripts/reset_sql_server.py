"""Reset the SQL Server side of the demo to the canonical pre-rehearsal state.

This is one of three partial-reset scripts that ``make reset`` calls. The
pipeline is:

  1. Assert ``--target`` is ``dev`` (script aborts otherwise — never touches
     ``directsales_prod`` or any other catalog target).
  2. ``TRUNCATE crm.consultoras`` against the throwaway SQL Server. This
     sidesteps the script-04 idempotency drift identified in Slice 04: a
     fresh empty table means script 03 reseeds every row and script 04 has
     a clean slate to apply the 2026-02-01 hero transition + the 50 backdated
     promotions.

     TRUNCATE on a CHANGE_TRACKING-enabled table also resets the per-table
     Change Tracking version, which is the right starting state for the
     Lakeflow Connect ingestion pipeline's next ``full_refresh`` snapshot.
  3. ``databricks bundle run sqlserver_setup -t dev`` — the 5-task Job
     that runs ``create_uc_connection`` -> ``phase_1_seed`` ->
     ``lfc_initial_sync`` -> ``phase_2_transitions`` -> ``lfc_post_transition_sync``
     (Slice 04). The two LFC sync tasks bracketing ``phase_2_transitions``
     produce the two distinct change events that Slice 06's AutoCDC SCD2
     needs to build hero #42's bronze→prata SCD2 history in
     ``gold.dim_consultora``.

     **Slice 04 idempotency cross-reference**: with the data_cadastro=2025-09-01
     pin in ``src/seed/generate_consultoras_sql.py`` and the
     ``updated_at < '2025-11-01'`` guard in ``04_apply_tier_transitions.sql``,
     re-running script 04 is a no-op. Combined with the TRUNCATE above, the
     bulk-tier promotion now lands on the same 48 rows every reset (the 50
     id%10==0 candidates minus the 2 seeded at diamante, which the script's
     own WHERE excludes — diamante is the top tier, nowhere to go).
  4. Verify hero #42 is at ``tier='prata'`` with ``updated_at='2026-02-01'``
     and the 48 transitioned candidates land at ``updated_at='2025-11-01'``.

Connection details come from the bundle's variables (``var.sqlserver_*``) so
this script and ``sqlserver_setup`` Job stay in lock-step. Credentials live
in the workspace secret scope ``directsales-demo`` (Slice 02) and are never read
locally — only the bundle's Job task reads them.

Usage::

    python scripts/reset_sql_server.py
    python scripts/reset_sql_server.py --target dev --yes  # (skip prompt)

Auth: SQL Server credentials are read from the local ``$SQLSERVER_SA_USER``
and ``$SQLSERVER_SA_PASSWORD`` env vars (so they're never committed). Set
them once for your shell. The bundle Job uses workspace secrets and is
unaffected.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from typing import Sequence

# Default connection target — matches databricks.yml's var.sqlserver_*.
# Kept here as constants because this script runs locally, separate from the
# bundle's variable resolution. If you change databricks.yml, change here too.
DEFAULT_HOST: str = "<SQLSERVER_HOST>"
DEFAULT_PORT: int = 1433
DEFAULT_DATABASE: str = "DemoDB"

# Bundle resource name for the 5-task Job (resources/jobs/sqlserver_setup.yml).
SQLSERVER_SETUP_JOB: str = "sqlserver_setup"

# Expected end state — used by the post-reset verification step.
EXPECTED_TOTAL_CONSULTORAS: int = 500
EXPECTED_HERO_ID: int = 42
EXPECTED_HERO_TIER_AFTER: str = "prata"
EXPECTED_HERO_UPDATED_AT: str = "2026-02-01 00:00:00"
# Of 50 id%10==0 candidates (excluding hero), 2 are seeded at diamante and
# excluded by script 04's WHERE; the other 48 land at 2025-11-01. See
# the file header for the full reasoning.
EXPECTED_TRANSITIONED_COUNT: int = 48
EXPECTED_TRANSITION_DATE: str = "2025-11-01 00:00:00"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reset the SQL Server side of the demo: TRUNCATE crm.consultoras "
            "+ run the 5-task sqlserver_setup Job. Restores hero #42's "
            "canonical pre-rehearsal state."
        )
    )
    parser.add_argument(
        "--target",
        default="dev",
        choices=["dev"],
        help=(
            "Bundle target. Locked to 'dev' — this script must never run "
            "against directsales_prod (PRD AC 36)."
        ),
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"SQL Server host (default: {DEFAULT_HOST}).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"SQL Server port (default: {DEFAULT_PORT}).",
    )
    parser.add_argument(
        "--database",
        default=DEFAULT_DATABASE,
        help=f"SQL Server database name (default: {DEFAULT_DATABASE}).",
    )
    parser.add_argument(
        "--schema",
        default="crm_dev",
        choices=["crm_dev", "crm_prod"],
        help=(
            "SQL Server schema to operate on (default: crm_dev). "
            "Locked to crm_dev for reset — crm_prod is refused at runtime."
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help=(
            "Skip the 5-second confirmation prompt before TRUNCATE. The "
            "Makefile's `reset-sql-server` target passes this. Run without "
            "it for an interactive sanity check."
        ),
    )
    parser.add_argument(
        "--skip-bundle-run",
        action="store_true",
        help=(
            "Truncate but do NOT run sqlserver_setup. Useful when the "
            "downstream Job will be triggered separately (e.g. by "
            "reset_databricks.py later in the same `make reset`)."
        ),
    )
    return parser.parse_args(argv)


def _assert_target_dev(target: str) -> None:
    """All destructive demo-reset operations must assert target == 'dev'.

    PRD AC 36. argparse already restricts ``--target`` to ``{'dev'}`` so this
    is belt-and-braces, but it makes the intent loud at the top of the script.
    """
    if target != "dev":
        raise SystemExit(
            f"refusing to reset SQL Server with target={target!r}; "
            "this script is hardcoded to target='dev' (PRD AC 36)."
        )


def _assert_schema_dev(schema: str) -> None:
    """Reset is a dev-only operation — refuse crm_prod unconditionally.

    Mirrors _assert_target_dev: argparse already restricts --schema to
    {'crm_dev', 'crm_prod'}, but this makes the intent explicit and ensures
    crm_prod can never be truncated via this script path.
    """
    if schema == "crm_prod":
        raise SystemExit(
            "refusing to reset SQL Server with schema='crm_prod'; "
            "reset is a dev-only operation. Use crm_dev."
        )


def _confirm(yes: bool) -> None:
    """5-second confirmation prompt (skip with ``--yes``).

    The 5-second pause is deliberate: enough time to ctrl-C if you triggered
    the wrong window, short enough that the reset Makefile flow doesn't drag.
    """
    if yes:
        return
    print(
        "About to TRUNCATE crm.consultoras on the demo SQL Server "
        f"({DEFAULT_HOST}:{DEFAULT_PORT}/{DEFAULT_DATABASE})."
    )
    print("Press Ctrl-C in the next 5 seconds to abort.")
    for i in range(5, 0, -1):
        print(f"  {i}…", flush=True)
        time.sleep(1)


def _resolve_credentials() -> tuple[str, str]:
    """Read SA user/password from environment.

    Never commit these. ``sql_server/INSTANCE.md`` documents the throwaway-
    instance credentials for prep, but the local script reads from the env
    so the values don't have to live in any file checked into git.
    """
    user = os.environ.get("SQLSERVER_SA_USER")
    password = os.environ.get("SQLSERVER_SA_PASSWORD")
    if not user or not password:
        raise SystemExit(
            "SQLSERVER_SA_USER and SQLSERVER_SA_PASSWORD env vars must be "
            "set. See sql_server/INSTANCE.md for the throwaway-instance "
            "credentials, then `export SQLSERVER_SA_USER=SA "
            "SQLSERVER_SA_PASSWORD=<pwd>` once per shell session."
        )
    return user, password


def _truncate_consultoras(host: str, port: int, database: str, schema: str) -> None:
    """Truncate ``<schema>.consultoras``.

    Uses ``python-tds`` (pure-Python TDS implementation) — same library as
    the ``sqlserver_setup`` notebook (Slice 02) so locally we never need
    Microsoft ODBC drivers or libsybdb.

    TRUNCATE rather than DELETE because TRUNCATE on a CHANGE_TRACKING-enabled
    table also resets the per-table Change Tracking version, which gives the
    LFC ingestion gateway a clean cursor to start from. DELETE leaves CT
    rows behind that confuse downstream SCD2 reconciliation.
    """
    user, password = _resolve_credentials()

    try:
        import pytds  # type: ignore[import-not-found]
    except ImportError as exc:
        raise SystemExit(
            "python-tds is not installed; run `pip install python-tds` "
            "(it's pinned in requirements-dev.txt)."
        ) from exc

    print(f"Connecting to {host}:{port} (database={database}) as {user}…")
    conn = pytds.connect(
        server=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=True,
        login_timeout=30,
        timeout=60,
    )
    try:
        cursor = conn.cursor()
        print(f"Executing TRUNCATE TABLE {schema}.consultoras…")
        cursor.execute(f"TRUNCATE TABLE {schema}.consultoras")
        cursor.close()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.consultoras")
        post_count = cursor.fetchone()[0]
        cursor.close()
        if post_count != 0:
            raise SystemExit(
                f"TRUNCATE did not empty {schema}.consultoras (post-truncate "
                f"count={post_count}). Investigate before proceeding."
            )
        print(f"{schema}.consultoras is now empty.")
    finally:
        conn.close()


def _bundle_run_sqlserver_setup(target: str) -> None:
    """Run the 5-task ``sqlserver_setup`` Job via the Databricks CLI.

    Subprocess'ing ``databricks bundle run`` is the right granularity: the
    Job already encodes the canonical 5-task sequence (create_uc_connection
    -> phase_1_seed -> lfc_initial_sync -> phase_2_transitions ->
    lfc_post_transition_sync) and we don't want to re-implement orchestration
    in two places. ``--no-wait`` is NOT used: the reset is synchronous; we
    want a single happy-path command that completes only when the Job has.
    """
    cmd = [
        "databricks",
        "bundle",
        "run",
        SQLSERVER_SETUP_JOB,
        "-t",
        target,
    ]
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise SystemExit(
            f"`databricks bundle run {SQLSERVER_SETUP_JOB} -t {target}` "
            f"failed with exit code {result.returncode}. Inspect the Job's "
            "task logs in the workspace (the failing task name is in the "
            "CLI output above)."
        )
    print(f"sqlserver_setup ({target}) completed.")


def _verify_post_reset(host: str, port: int, database: str, schema: str) -> None:
    """Post-reset regression checks (Slice 04 idempotency-fix verification).

    Runs three queries against SQL Server:

    1. Total consultora count == 500
    2. Hero #42 is at the post-transition tier ('prata') with updated_at ==
       2026-02-01.
    3. The 48 backdated tier-transition rows landed at updated_at ==
       2025-11-01.

    Failing any of these means the reset path is broken (most likely script
    04's idempotency guard regressed, or the Job's order-of-tasks changed).
    """
    user, password = _resolve_credentials()
    import pytds  # type: ignore[import-not-found]

    conn = pytds.connect(
        server=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=True,
        login_timeout=30,
        timeout=60,
    )
    failures: list[str] = []
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.consultoras")
        n_total = cursor.fetchone()[0]
        cursor.close()
        if n_total != EXPECTED_TOTAL_CONSULTORAS:
            failures.append(
                f"total consultoras={n_total}, "
                f"expected {EXPECTED_TOTAL_CONSULTORAS}"
            )

        # Constants are baked into the SQL deliberately. python-tds's default
        # paramstyle is pyformat (%(name)s / %s); we'd have to thread it
        # through every callsite to use it safely. Verification queries
        # here are read-only against constants we control, so literal
        # substitution is unambiguous and side-effect-free.
        cursor = conn.cursor()
        cursor.execute(
            "SELECT tier, CONVERT(VARCHAR(30), updated_at, 120) "
            f"FROM {schema}.consultoras WHERE consultora_id = {EXPECTED_HERO_ID}"
        )
        hero_row = cursor.fetchone()
        cursor.close()
        if hero_row is None:
            failures.append(f"hero #{EXPECTED_HERO_ID} not found in seed")
        else:
            tier, updated_at = hero_row
            if tier != EXPECTED_HERO_TIER_AFTER:
                failures.append(
                    f"hero tier={tier!r}, "
                    f"expected {EXPECTED_HERO_TIER_AFTER!r}"
                )
            if not str(updated_at).startswith(EXPECTED_HERO_UPDATED_AT):
                failures.append(
                    f"hero updated_at={updated_at!r}, "
                    f"expected starts with {EXPECTED_HERO_UPDATED_AT!r}"
                )

        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM {schema}.consultoras "
            f"WHERE consultora_id <> {EXPECTED_HERO_ID} "
            "AND consultora_id % 10 = 0 "
            f"AND updated_at = '{EXPECTED_TRANSITION_DATE}'"
        )
        n_transitioned = cursor.fetchone()[0]
        cursor.close()
        if n_transitioned != EXPECTED_TRANSITIONED_COUNT:
            failures.append(
                f"transitioned consultoras={n_transitioned}, "
                f"expected {EXPECTED_TRANSITIONED_COUNT}"
            )
    finally:
        conn.close()

    if failures:
        msg = "\n  ".join(failures)
        raise SystemExit(
            "Post-reset verification failed:\n  " + msg + "\n"
            "Inspect the sqlserver_setup Job's tasks in the workspace and "
            "the seed/04 SQL files. Do NOT proceed with reset_volume / "
            "reset_databricks until this is resolved."
        )
    print(
        f"Post-reset verification OK: {EXPECTED_TOTAL_CONSULTORAS} consultoras, "
        f"hero #{EXPECTED_HERO_ID} at {EXPECTED_HERO_TIER_AFTER!r}, "
        f"{EXPECTED_TRANSITIONED_COUNT} transitioned."
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    _assert_target_dev(args.target)
    _assert_schema_dev(args.schema)
    _confirm(args.yes)
    _truncate_consultoras(args.host, args.port, args.database, args.schema)

    if args.skip_bundle_run:
        print(
            "Skipping `databricks bundle run sqlserver_setup` (--skip-bundle-run). "
            "Caller is responsible for triggering it."
        )
        return 0

    _bundle_run_sqlserver_setup(args.target)
    _verify_post_reset(args.host, args.port, args.database, args.schema)
    return 0


if __name__ == "__main__":
    sys.exit(main())
