"""Reset the bronze landing-zone volume to a clean 50 000-row Pedido baseline.

Wipes every parquet file in ``/Volumes/${target_catalog}/bronze/lz/pedidos/``
(including the Slice 09 ``pedidos_canal_origem_*.parquet`` smoke drops, which
must be cleared before the next live demo so the schema-evolution beat looks
fresh) and regenerates the canonical 50 files via
``src/seed/generate_pedidos_parquet.generate(seed=42, n_pedidos=50_000)``.

This script does **not** trigger the SDP pipeline. The new files sit in the
volume until ``reset_databricks.py`` runs ``full_refresh: true`` on the
pipeline. That separation keeps the volume reset fast (~10s) and lets
``make reset-volume`` be a one-shot recovery tool when only the parquet
landing zone got dirty (e.g. between-rehearsal smoke drops).

All destructive actions assert ``--target == 'dev'`` (PRD AC 36); the
``directsales_prod`` volume is never reachable from this script.

Usage::

    python scripts/reset_volume.py
    python scripts/reset_volume.py --yes  # skip confirmation
    python scripts/reset_volume.py --target dev --target-catalog directsales_dev
"""
from __future__ import annotations

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Sequence

# Allow `python scripts/reset_volume.py` from the repo root without
# fiddling with PYTHONPATH. The project does not install as a package; src/
# is a regular folder relative to repo root (parent of scripts/). Same
# pattern as scripts/demo_schema_evolution.py.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.seed.constants import N_PEDIDOS_DEFAULT  # noqa: E402
from src.seed.generate_pedidos_parquet import generate as generate_pedidos  # noqa: E402

DEFAULT_TARGET_CATALOG: str = "directsales_dev"
VOLUME_TEMPLATE: str = "/Volumes/{catalog}/bronze/lz/pedidos"
DEFAULT_SEED: int = 42  # Matches src.seed.generate_pedidos_parquet.generate's default.


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reset the bronze parquet landing zone: wipe all files in the "
            "pedidos volume, regenerate the canonical 50 000-row dataset, "
            "and re-upload."
        )
    )
    parser.add_argument(
        "--target",
        default="dev",
        choices=["dev"],
        help=(
            "Bundle target. Locked to 'dev'; the directsales_prod volume is "
            "never touched by this script (PRD AC 36)."
        ),
    )
    parser.add_argument(
        "--target-catalog",
        default=DEFAULT_TARGET_CATALOG,
        help=f"Unity Catalog name (default: {DEFAULT_TARGET_CATALOG}).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=(
            f"Seed for the generator (default: {DEFAULT_SEED}, matches "
            "src.seed.generate_pedidos_parquet)."
        ),
    )
    parser.add_argument(
        "--n-pedidos",
        type=int,
        default=N_PEDIDOS_DEFAULT,
        help=(
            f"Total Pedidos to generate (default: {N_PEDIDOS_DEFAULT}). "
            "Production demo always uses the default."
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help=(
            "Skip the 5-second confirmation prompt before deleting files. "
            "The Makefile's `reset-volume` target passes this."
        ),
    )
    return parser.parse_args(argv)


def _assert_target_dev(target: str, target_catalog: str) -> None:
    """Refuse to touch anything but ``directsales_dev`` (PRD AC 36).

    ``argparse`` already restricts ``--target`` to ``{'dev'}``, but a typo'd
    ``--target-catalog directsales_prod`` would still slip through that check
    alone. Defense in depth: assert the catalog name is the dev catalog.
    """
    if target != "dev":
        raise SystemExit(
            f"refusing to reset volume with target={target!r}; "
            "this script is hardcoded to target='dev' (PRD AC 36)."
        )
    if target_catalog == "directsales_prod":
        raise SystemExit(
            f"refusing to reset volume on target_catalog={target_catalog!r}; "
            "this script must never run against directsales_prod (PRD AC 36)."
        )


def _confirm(yes: bool, volume_path: str) -> None:
    """5-second confirmation prompt (skip with ``--yes``)."""
    if yes:
        return
    print(
        f"About to delete every file in {volume_path} and regenerate the "
        f"canonical {N_PEDIDOS_DEFAULT}-row Pedido parquet set."
    )
    print("Press Ctrl-C in the next 5 seconds to abort.")
    for i in range(5, 0, -1):
        print(f"  {i}…", flush=True)
        time.sleep(1)


def _list_volume_files(client, volume_path: str) -> list[str]:
    """Return absolute paths of every regular file under ``volume_path``.

    Non-recursive — the bronze landing zone is a flat folder of parquet
    files (Slice 03 design). If a future slice nests subfolders, this
    will need a recursive walk.

    Treats NotFound on the directory itself as "no files yet". This
    happens after ``reset_databricks.py`` drops + recreates the bronze
    schema: the bundle deploy recreates the ``bronze.lz`` volume but the
    ``pedidos/`` subfolder inside it is only materialized when the first
    file is uploaded. A fresh-from-deploy volume legitimately has an
    empty (uncreated) subfolder.
    """
    from databricks.sdk.errors.platform import NotFound

    paths: list[str] = []
    # list_directory_contents returns an iterator; the underlying API call
    # (and NotFound) fires only when iteration starts. Materialize via list()
    # inside the try so the exception is actually caught.
    try:
        entries = list(client.files.list_directory_contents(volume_path))
    except NotFound:
        return paths
    for entry in entries:
        if entry.is_directory:
            # No-op for now; bronze landing zone is flat. Keep the guard
            # so the script's intent is explicit if subfolders appear.
            continue
        paths.append(entry.path)
    return paths


def _wipe_volume(volume_path: str) -> int:
    """Delete every parquet file currently in the volume.

    Returns the number of files deleted. Uses ``WorkspaceClient.files`` —
    same pattern as scripts/demo_schema_evolution.py's upload step. No
    timestamp filter here: the goal is full clean state, including any
    previous smoke uploads (canal_origem live-beat artefacts, manual
    drops, half-finished previous reset runs).
    """
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    print(f"Listing files in {volume_path}…")
    paths = _list_volume_files(client, volume_path)
    print(f"  found {len(paths)} files.")

    for p in paths:
        client.files.delete(file_path=p)
    print(f"Deleted {len(paths)} files.")
    return len(paths)


def _regenerate_and_upload(
    *,
    volume_path: str,
    seed: int,
    n_pedidos: int,
) -> int:
    """Regenerate the 50-file Pedido set in a tempdir and upload to volume.

    Uses the exact same generator the live demo's first deploy used, with
    the same default seed/n_pedidos, so the post-reset bronze state is
    byte-identical to the canonical baseline (modulo file metadata).
    """
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    with tempfile.TemporaryDirectory() as tmp:
        print(
            f"Generating {n_pedidos} Pedidos to a tempdir (seed={seed})…"
        )
        result = generate_pedidos(
            seed=seed,
            n_pedidos=n_pedidos,
            output_dir=tmp,
        )
        print(
            f"  wrote {len(result.files_written)} parquet files; "
            f"hero before/after transition: "
            f"{result.hero_pedidos_before_transition}/"
            f"{result.hero_pedidos_after_transition}; "
            f"planted bad rows: "
            f"{result.n_planted_negative_valor} negative valor + "
            f"{result.n_planted_future_date} future date."
        )

        print(f"Uploading to {volume_path}…")
        for parquet_file in sorted(Path(tmp).glob("*.parquet")):
            target = f"{volume_path}/{parquet_file.name}"
            with parquet_file.open("rb") as fh:
                # overwrite=True so a partial previous run that left a
                # subset behind doesn't block this re-upload.
                client.files.upload(
                    file_path=target, contents=fh, overwrite=True
                )
        print(f"Uploaded {len(result.files_written)} parquet files.")
        return len(result.files_written)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    _assert_target_dev(args.target, args.target_catalog)
    volume_path = VOLUME_TEMPLATE.format(catalog=args.target_catalog)
    _confirm(args.yes, volume_path)

    n_deleted = _wipe_volume(volume_path)
    n_uploaded = _regenerate_and_upload(
        volume_path=volume_path,
        seed=args.seed,
        n_pedidos=args.n_pedidos,
    )
    print(
        f"reset_volume complete: {n_deleted} files deleted, "
        f"{n_uploaded} files uploaded. The SDP pipeline must be run with "
        "full_refresh=true to re-snapshot bronze (handled by "
        "reset_databricks.py)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
