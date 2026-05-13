"""Synthetic Pedido parquet generator with ``canal_origem`` — Slice 09 live beat asset.

A small (~100 row) parquet file with all base Pedido columns plus a NEW
``canal_origem`` column. Auto Loader on ``bronze.pedidos_raw`` runs with
``schemaEvolutionMode = 'addNewColumns'`` (Slice 03), so dropping this file
into the bronze landing zone causes the bronze streaming table to widen
automatically. Silver propagates on the same update because Slice 09 also
reshaped ``silver.pedidos`` to use ``SELECT * EXCEPT (...)``.

Distribution per PRD: 60 % ``app``, 30 % ``web``, 10 % ``whatsapp``.

The companion ``scripts/demo_schema_evolution.py`` wraps this generator and
uploads the result to the UC Volume in one step. Same ``seed`` + same
``today`` produces byte-identical output (so smoke runs are reproducible),
but the filename embeds the seed + date so successive demo runs land
distinct files in the volume.

Pedido IDs default to a high range (``1_000_000+``) to avoid colliding with
the main 50 000-row generator's ``1..50_000``.

CLI
---
::

   python -m src.seed.generate_canal_origem_pedidos --output-dir /tmp --seed 909
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from src.seed.constants import (
    DATA_PEDIDO_WINDOW_MONTHS,
    FORMA_PAGAMENTO_DISTRIBUTION,
    N_CONSULTORAS,
    STATUS_DISTRIBUTION,
    VALOR_TOTAL_LOGNORMAL_MEAN,
    VALOR_TOTAL_LOGNORMAL_SIGMA,
)

CANAL_ORIGEM_DISTRIBUTION: dict[str, float] = {
    "app": 0.60,
    "web": 0.30,
    "whatsapp": 0.10,
}

# Default starting pedido_id for canal_origem rows. Sits well above the main
# generator's 1..50_000 range so the bronze table never sees overlapping IDs.
DEFAULT_PEDIDO_ID_START: int = 1_000_000

DEFAULT_N_PEDIDOS: int = 100
DEFAULT_SEED: int = 909


@dataclass
class CanalOrigemGenerateResult:
    """Summary of one canal_origem generation run."""

    output_path: Path
    n_pedidos: int
    canal_counts: dict[str, int]


def generate(
    *,
    seed: int = DEFAULT_SEED,
    n_pedidos: int = DEFAULT_N_PEDIDOS,
    output_dir: Path | str,
    today: datetime | None = None,
    pedido_id_start: int = DEFAULT_PEDIDO_ID_START,
    n_consultoras: int = N_CONSULTORAS,
) -> CanalOrigemGenerateResult:
    """Generate one canal_origem parquet file deterministically.

    Args:
        seed: Random seed (numpy). Same seed + same ``today`` is byte-identical.
        n_pedidos: Total rows in the generated parquet. Default 100.
        output_dir: Destination directory for the parquet file (created if absent).
        today: End anchor for the 24-month ``data_pedido`` window. Defaults
            to UTC midnight today; tests should pass an explicit value.
        pedido_id_start: Starting pedido_id (avoids colliding with the main
            generator's 1..50_000 range).
        n_consultoras: Pool size for ``consultora_id`` (1..n_consultoras).

    Returns:
        A :class:`CanalOrigemGenerateResult` with the parquet path + per-canal counts.
    """
    if n_pedidos <= 0:
        raise ValueError("n_pedidos must be positive")

    today_dt = (
        today or datetime.now(tz=timezone.utc).replace(tzinfo=None)
    ).replace(hour=0, minute=0, second=0, microsecond=0)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)

    statuses = list(STATUS_DISTRIBUTION.keys())
    status_p = list(STATUS_DISTRIBUTION.values())
    pagamentos = list(FORMA_PAGAMENTO_DISTRIBUTION.keys())
    pagamento_p = list(FORMA_PAGAMENTO_DISTRIBUTION.values())
    canais = list(CANAL_ORIGEM_DISTRIBUTION.keys())
    canal_p = list(CANAL_ORIGEM_DISTRIBUTION.values())

    window_start = today_dt - timedelta(days=DATA_PEDIDO_WINDOW_MONTHS * 30)

    pedido_id = list(range(pedido_id_start, pedido_id_start + n_pedidos))
    consultora_id = rng.integers(
        low=1, high=n_consultoras + 1, size=n_pedidos, dtype=np.int32
    ).tolist()
    days_offset = rng.integers(
        low=0, high=DATA_PEDIDO_WINDOW_MONTHS * 30, size=n_pedidos
    )
    data_pedido = [window_start + timedelta(days=int(d)) for d in days_offset]
    valor_total = np.round(
        rng.lognormal(
            mean=VALOR_TOTAL_LOGNORMAL_MEAN,
            sigma=VALOR_TOTAL_LOGNORMAL_SIGMA,
            size=n_pedidos,
        ),
        2,
    ).tolist()
    status = rng.choice(statuses, size=n_pedidos, p=status_p).tolist()
    forma_pagamento = rng.choice(pagamentos, size=n_pedidos, p=pagamento_p).tolist()
    updated_at = [
        d + timedelta(hours=int(h))
        for d, h in zip(
            data_pedido,
            rng.integers(low=0, high=72, size=n_pedidos),
            strict=True,
        )
    ]
    canal_origem = rng.choice(canais, size=n_pedidos, p=canal_p).tolist()

    schema = pa.schema(
        [
            ("pedido_id", pa.int64()),
            ("consultora_id", pa.int32()),
            ("data_pedido", pa.timestamp("us")),
            ("valor_total", pa.float64()),
            ("status", pa.string()),
            ("forma_pagamento", pa.string()),
            ("updated_at", pa.timestamp("us")),
            ("canal_origem", pa.string()),
        ]
    )

    table = pa.table(
        {
            "pedido_id": pedido_id,
            "consultora_id": consultora_id,
            "data_pedido": data_pedido,
            "valor_total": valor_total,
            "status": status,
            "forma_pagamento": forma_pagamento,
            "updated_at": updated_at,
            "canal_origem": canal_origem,
        },
        schema=schema,
    )

    filename = filename_for(seed=seed, today=today_dt)
    output_path = output_dir / filename
    pq.write_table(table, output_path, compression="snappy")

    canal_counts = {c: int(canal_origem.count(c)) for c in canais}

    return CanalOrigemGenerateResult(
        output_path=output_path,
        n_pedidos=n_pedidos,
        canal_counts=canal_counts,
    )


def filename_for(*, seed: int, today: datetime) -> str:
    """Build a stable filename so successive demo drops don't collide.

    Embedding ``seed`` keeps test runs reproducible while still letting demo
    drops stay distinct (the demo script defaults to a fresh seed each call).
    """
    return f"pedidos_canal_origem_{today.strftime('%Y%m%d')}_{seed:06d}.parquet"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a small canal_origem parquet for the Slice 09 schema-evolution "
            "live beat."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Local directory for the parquet output (created if absent).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed (default: {DEFAULT_SEED}).",
    )
    parser.add_argument(
        "--n-pedidos",
        type=int,
        default=DEFAULT_N_PEDIDOS,
        help=f"Row count (default: {DEFAULT_N_PEDIDOS}).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    result = generate(
        seed=args.seed,
        n_pedidos=args.n_pedidos,
        output_dir=args.output_dir,
    )
    print(
        f"Wrote {result.output_path.name} ({result.n_pedidos} rows) to "
        f"{result.output_path.parent}. Canal distribution: {result.canal_counts}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
