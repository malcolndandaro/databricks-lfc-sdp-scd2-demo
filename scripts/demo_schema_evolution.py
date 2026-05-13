"""Drop a canal_origem parquet into the bronze landing zone — the Slice 09 live beat.

Generates a small (~100 row) parquet file with a NEW ``canal_origem`` column
and uploads it to ``/Volumes/${target_catalog}/bronze/lz/pedidos/`` via the
Databricks SDK. The Auto Loader source on ``bronze.pedidos_raw`` runs with
``schemaEvolutionMode = 'addNewColumns'`` (Slice 03), so the next streaming
trigger picks up the new column and widens bronze automatically. Silver
propagates on the same update because Slice 09 reshaped ``silver.pedidos``
to use ``SELECT * EXCEPT (...)``.

Usage
-----
::

    python scripts/demo_schema_evolution.py
    python scripts/demo_schema_evolution.py --target-catalog directsales_dev --seed 909
    python scripts/demo_schema_evolution.py --dry-run

Auth: uses your active Databricks workspace profile via ``WorkspaceClient()``.

Expected timing: ~30 seconds from upload to visible widening in
``bronze.pedidos_raw``, assuming the SDP pipeline is in CONTINUOUS mode or
gets triggered shortly after. See ``scripts/README.md`` for the full
manual-smoke checklist.
"""
from __future__ import annotations

import argparse
import secrets
import sys
import tempfile
from pathlib import Path
from typing import Sequence

# Allow `python scripts/demo_schema_evolution.py` from the repo root without
# fiddling with PYTHONPATH. The project does not install as a package; src/
# is just a regular folder relative to the repo root (the parent of scripts/).
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.seed.generate_canal_origem_pedidos import (  # noqa: E402
    DEFAULT_N_PEDIDOS,
    generate as generate_canal_origem,
)

# Default volume path matches databricks.yml var.source_volume_path for dev.
DEFAULT_TARGET_CATALOG: str = "directsales_dev"
VOLUME_TEMPLATE: str = "/Volumes/{catalog}/bronze/lz/pedidos"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Slice 09 live beat: drop a canal_origem parquet into the bronze "
            "landing zone and watch Auto Loader widen the schema."
        )
    )
    parser.add_argument(
        "--target-catalog",
        default=DEFAULT_TARGET_CATALOG,
        help=(
            f"Unity Catalog name (default: {DEFAULT_TARGET_CATALOG}). The volume "
            f"path is built as {VOLUME_TEMPLATE}."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help=(
            "Random seed. Defaults to a fresh value per invocation so successive "
            "demo drops produce distinct filenames; pass an explicit seed for "
            "reproducible smoke tests."
        ),
    )
    parser.add_argument(
        "--n-pedidos",
        type=int,
        default=DEFAULT_N_PEDIDOS,
        help=f"Row count for the new file (default: {DEFAULT_N_PEDIDOS}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Generate the parquet locally and print the would-be volume path, "
            "but skip the upload."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    seed = args.seed if args.seed is not None else secrets.randbelow(10**6)
    volume_path = VOLUME_TEMPLATE.format(catalog=args.target_catalog)

    with tempfile.TemporaryDirectory() as tmp:
        result = generate_canal_origem(
            seed=seed,
            n_pedidos=args.n_pedidos,
            output_dir=tmp,
        )
        target = f"{volume_path}/{result.output_path.name}"
        print(
            f"Generated {result.output_path.name} "
            f"({result.n_pedidos} rows). Canal distribution: {result.canal_counts}."
        )

        if args.dry_run:
            print(f"[dry-run] Would upload to {target}.")
            return 0

        # Imported lazily so --dry-run works without databricks-sdk installed.
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        with result.output_path.open("rb") as fh:
            client.files.upload(file_path=target, contents=fh, overwrite=False)
        print(f"Uploaded -> {target}")
        print(
            "The Auto Loader source on bronze.pedidos_raw will pick up the new "
            "column on its next trigger (~30s for serverless SDP on the demo "
            "workspace). silver.pedidos propagates on the same update."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
