"""Synthetic Pedido parquet generator — the deep module.

Public interface
----------------
:func:`generate` is the single entry point. It writes ~50 parquet files of
1 000 rows each (50 000 Pedidos total by default) into ``output_dir``.

Same ``seed`` + same ``today`` produces byte-identical output. Distributions
match the PRD spec within ±5 %. Hero ``consultora_id = 42`` always has
:data:`HERO_PEDIDOS_TOTAL` Pedidos, half before :data:`HERO_TRANSITION_DATE`
and half after — this is what makes the as-of comissão MV (Slice 07) interesting.

Bad rows are planted deterministically so the silver Expectations beat
(Slice 05) shows non-zero violation counts in the SDP Quality tab:

* :data:`BAD_NEGATIVE_VALOR_COUNT` Pedidos with ``valor_total = -1``
* :data:`BAD_FUTURE_DATE_COUNT` Pedidos with ``data_pedido`` in
  ``BAD_FUTURE_YEAR``

CLI
---
::

   python -m src.seed.generate_pedidos_parquet --output-dir /tmp/demo-data --seed 42

Pass ``--upload-to /Volumes/directsales_dev/bronze/lz/pedidos`` to also push the
generated files into a UC Volume via the Databricks SDK (workspace auth must
be configured). The reset flow (Slice 12) drives this combined invocation.
"""
from __future__ import annotations

import argparse
import io
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

from src.seed.constants import (
    BAD_FUTURE_DATE_COUNT,
    BAD_FUTURE_YEAR,
    BAD_NEGATIVE_VALOR_COUNT,
    DATA_PEDIDO_WINDOW_MONTHS,
    FORMA_PAGAMENTO_DISTRIBUTION,
    HERO_CONSULTORA_ID,
    HERO_PEDIDOS_TOTAL,
    HERO_TRANSITION_DATE,
    N_CONSULTORAS,
    N_PEDIDOS_DEFAULT,
    PEDIDOS_PER_FILE,
    STATUS_DISTRIBUTION,
    VALOR_TOTAL_LOGNORMAL_MEAN,
    VALOR_TOTAL_LOGNORMAL_SIGMA,
)


@dataclass
class GenerateResult:
    """Summary of one generation run.

    Useful for tests + the CLI summary line. Counts are exact, not estimates.
    """

    output_dir: Path
    files_written: list[Path]
    n_pedidos: int
    n_planted_negative_valor: int
    n_planted_future_date: int
    hero_pedidos_before_transition: int
    hero_pedidos_after_transition: int


def generate(
    *,
    seed: int = 42,
    n_pedidos: int = N_PEDIDOS_DEFAULT,
    output_dir: Path | str,
    today: datetime | None = None,
    pedidos_per_file: int = PEDIDOS_PER_FILE,
    n_consultoras: int = N_CONSULTORAS,
) -> GenerateResult:
    """Generate Pedido parquet files deterministically.

    Same ``seed`` + same ``today`` yields the same output (modulo filesystem
    metadata). Distributions follow the PRD spec.

    Args:
        seed: Random seed. Threaded into numpy, Faker and stdlib ``random``.
        n_pedidos: Total Pedido rows. Hero Pedidos are included in this total.
        output_dir: Destination dir for parquet files (created if absent).
        today: End anchor for the 24-month ``data_pedido`` window. Defaults
            to UTC midnight today; tests should pass an explicit value.
        pedidos_per_file: Rows per parquet file (last file may be shorter).
        n_consultoras: Pool size for ``consultora_id`` (1..n_consultoras).

    Returns:
        A :class:`GenerateResult` with file paths + planted-row counts.
    """
    if n_pedidos <= 0:
        raise ValueError("n_pedidos must be positive")
    if pedidos_per_file <= 0:
        raise ValueError("pedidos_per_file must be positive")
    if n_consultoras < HERO_CONSULTORA_ID:
        raise ValueError(
            f"n_consultoras={n_consultoras} cannot be smaller than "
            f"HERO_CONSULTORA_ID={HERO_CONSULTORA_ID}"
        )

    today_dt = (
        today
        or datetime.now(tz=timezone.utc).replace(tzinfo=None)
    ).replace(hour=0, minute=0, second=0, microsecond=0)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    py_rng = random.Random(seed)
    faker = Faker("pt_BR")
    faker.seed_instance(seed)

    rows = _build_rows(
        rng=rng,
        py_rng=py_rng,
        n_pedidos=n_pedidos,
        n_consultoras=n_consultoras,
        today_dt=today_dt,
    )

    table = _rows_to_arrow_table(rows)

    files_written = _write_partitioned_parquet(
        table=table,
        output_dir=output_dir,
        pedidos_per_file=pedidos_per_file,
    )

    return GenerateResult(
        output_dir=output_dir,
        files_written=files_written,
        n_pedidos=table.num_rows,
        n_planted_negative_valor=int(
            sum(1 for v in table["valor_total"].to_pylist() if v == -1.0)
        ),
        n_planted_future_date=int(
            sum(
                1
                for d in table["data_pedido"].to_pylist()
                if d.year == BAD_FUTURE_YEAR
            )
        ),
        hero_pedidos_before_transition=_count_hero(
            table, before=HERO_TRANSITION_DATE
        ),
        hero_pedidos_after_transition=_count_hero(
            table, after=HERO_TRANSITION_DATE
        ),
    )


def _build_rows(
    *,
    rng: np.random.Generator,
    py_rng: random.Random,
    n_pedidos: int,
    n_consultoras: int,
    today_dt: datetime,
) -> dict:
    """Build the column-oriented row dict used to construct the Arrow table.

    Hero rows are constructed first (deterministic dates around the transition);
    the remaining slots are filled with the random distribution. Bad rows are
    planted at fixed indices at the end so their positions are deterministic.
    """
    statuses = list(STATUS_DISTRIBUTION.keys())
    status_p = list(STATUS_DISTRIBUTION.values())
    pagamentos = list(FORMA_PAGAMENTO_DISTRIBUTION.keys())
    pagamento_p = list(FORMA_PAGAMENTO_DISTRIBUTION.values())

    window_start = today_dt - timedelta(days=DATA_PEDIDO_WINDOW_MONTHS * 30)

    hero_pedidos = _build_hero_pedidos(
        rng=rng, today_dt=today_dt, window_start=window_start
    )
    n_hero = len(hero_pedidos["consultora_id"])

    # Bad rows reserved at the tail of the id space so they're stable across
    # changes to n_pedidos. Only their *positions* are deterministic — values
    # are still drawn from RNG (except the planted invalid fields).
    n_bad = BAD_NEGATIVE_VALOR_COUNT + BAD_FUTURE_DATE_COUNT
    n_random = n_pedidos - n_hero - n_bad
    if n_random < 0:
        raise ValueError(
            f"n_pedidos={n_pedidos} is too small to hold "
            f"hero ({n_hero}) + bad ({n_bad}) Pedidos"
        )

    pedido_id = list(range(1, n_pedidos + 1))

    consultora_id = rng.integers(
        low=1, high=n_consultoras + 1, size=n_random, dtype=np.int32
    ).tolist()
    days_offset = rng.integers(
        low=0, high=DATA_PEDIDO_WINDOW_MONTHS * 30, size=n_random
    )
    data_pedido_random = [
        window_start + timedelta(days=int(d)) for d in days_offset
    ]
    valor_total_random = np.round(
        rng.lognormal(
            mean=VALOR_TOTAL_LOGNORMAL_MEAN,
            sigma=VALOR_TOTAL_LOGNORMAL_SIGMA,
            size=n_random,
        ),
        2,
    ).tolist()
    status_random = rng.choice(statuses, size=n_random, p=status_p).tolist()
    forma_pagamento_random = rng.choice(
        pagamentos, size=n_random, p=pagamento_p
    ).tolist()

    # Bad rows: explicit invalid values, otherwise normal distributions.
    bad_consultora = rng.integers(
        low=1, high=n_consultoras + 1, size=n_bad, dtype=np.int32
    ).tolist()
    bad_status = rng.choice(statuses, size=n_bad, p=status_p).tolist()
    bad_pagamento = rng.choice(pagamentos, size=n_bad, p=pagamento_p).tolist()
    bad_valor = (
        [-1.0] * BAD_NEGATIVE_VALOR_COUNT
        + np.round(
            rng.lognormal(
                mean=VALOR_TOTAL_LOGNORMAL_MEAN,
                sigma=VALOR_TOTAL_LOGNORMAL_SIGMA,
                size=BAD_FUTURE_DATE_COUNT,
            ),
            2,
        ).tolist()
    )
    bad_data = [today_dt - timedelta(days=int(rng.integers(0, 365)))] * BAD_NEGATIVE_VALOR_COUNT + [
        datetime(BAD_FUTURE_YEAR, 6, 1) + timedelta(days=int(d))
        for d in rng.integers(0, 90, size=BAD_FUTURE_DATE_COUNT)
    ]

    consultora_id_full = (
        hero_pedidos["consultora_id"] + consultora_id + bad_consultora
    )
    data_pedido_full = (
        hero_pedidos["data_pedido"] + data_pedido_random + bad_data
    )
    valor_total_full = (
        hero_pedidos["valor_total"] + valor_total_random + bad_valor
    )
    status_full = hero_pedidos["status"] + status_random + bad_status
    forma_pagamento_full = (
        hero_pedidos["forma_pagamento"] + forma_pagamento_random + bad_pagamento
    )

    # updated_at: each Pedido was last touched some hours after data_pedido.
    updated_at_full = [
        d + timedelta(hours=int(h))
        for d, h in zip(
            data_pedido_full,
            rng.integers(low=0, high=72, size=n_pedidos),
            strict=True,
        )
    ]

    return {
        "pedido_id": pedido_id,
        "consultora_id": consultora_id_full,
        "data_pedido": data_pedido_full,
        "valor_total": valor_total_full,
        "status": status_full,
        "forma_pagamento": forma_pagamento_full,
        "updated_at": updated_at_full,
    }


def _build_hero_pedidos(
    *,
    rng: np.random.Generator,
    today_dt: datetime,
    window_start: datetime,
) -> dict:
    """Build hero Consultora #42's Pedidos: half before, half after the
    tier transition. All :data:`HERO_PEDIDOS_TOTAL` are real (no bad rows
    among hero Pedidos so MV2 attributes them cleanly to a tier).
    """
    transition_dt = datetime(
        HERO_TRANSITION_DATE.year,
        HERO_TRANSITION_DATE.month,
        HERO_TRANSITION_DATE.day,
    )
    half = HERO_PEDIDOS_TOTAL // 2

    # Spread "before" Pedidos evenly between window_start and the transition,
    # and "after" Pedidos between the transition and today.
    before_window_days = max(
        (transition_dt - window_start).days - 1, half
    )
    after_window_days = max((today_dt - transition_dt).days - 1, half)

    before_offsets = rng.choice(
        np.arange(0, before_window_days), size=half, replace=False
    )
    after_offsets = rng.choice(
        np.arange(1, after_window_days + 1), size=HERO_PEDIDOS_TOTAL - half, replace=False
    )

    before_dates = [window_start + timedelta(days=int(d)) for d in before_offsets]
    after_dates = [transition_dt + timedelta(days=int(d)) for d in after_offsets]
    data_pedido = before_dates + after_dates

    valores = np.round(
        rng.lognormal(
            mean=VALOR_TOTAL_LOGNORMAL_MEAN,
            sigma=VALOR_TOTAL_LOGNORMAL_SIGMA,
            size=HERO_PEDIDOS_TOTAL,
        ),
        2,
    ).tolist()

    return {
        "consultora_id": [HERO_CONSULTORA_ID] * HERO_PEDIDOS_TOTAL,
        "data_pedido": data_pedido,
        "valor_total": valores,
        # Hero Pedidos are mostly entregue so MV2 numbers are clean on stage.
        "status": ["entregue"] * HERO_PEDIDOS_TOTAL,
        "forma_pagamento": ["pix"] * HERO_PEDIDOS_TOTAL,
    }


def _rows_to_arrow_table(rows: dict) -> pa.Table:
    """Convert column-dict to a stable Arrow schema.

    Schema chosen to match what Auto Loader will infer + what silver.pedidos
    will type-cast to. Keeping ``valor_total`` as float64 in parquet (cast to
    decimal in silver) avoids pyarrow decimal serialisation quirks and keeps
    the demo's bad-row values (``-1.0``) representable.
    """
    schema = pa.schema(
        [
            ("pedido_id", pa.int64()),
            ("consultora_id", pa.int32()),
            ("data_pedido", pa.timestamp("us")),
            ("valor_total", pa.float64()),
            ("status", pa.string()),
            ("forma_pagamento", pa.string()),
            ("updated_at", pa.timestamp("us")),
        ]
    )
    return pa.table(rows, schema=schema)


def _write_partitioned_parquet(
    *,
    table: pa.Table,
    output_dir: Path,
    pedidos_per_file: int,
) -> list[Path]:
    """Slice the table into evenly-sized chunks and write each to its own
    parquet file. File names are ``pedidos_NNNN.parquet`` with stable
    zero-padding so directory listings sort consistently.
    """
    n = table.num_rows
    n_files = (n + pedidos_per_file - 1) // pedidos_per_file
    width = max(4, len(str(n_files)))

    paths: list[Path] = []
    for i in range(n_files):
        chunk = table.slice(i * pedidos_per_file, pedidos_per_file)
        path = output_dir / f"pedidos_{i:0{width}d}.parquet"
        pq.write_table(chunk, path, compression="snappy")
        paths.append(path)
    return paths


def _count_hero(
    table: pa.Table,
    *,
    before: object | None = None,
    after: object | None = None,
) -> int:
    """Count hero Pedidos whose ``data_pedido`` falls before or after a date."""
    consultora = table["consultora_id"].to_pylist()
    data_pedido = table["data_pedido"].to_pylist()
    cutoff = datetime(before.year, before.month, before.day) if before else (
        datetime(after.year, after.month, after.day) if after else None
    )
    if cutoff is None:
        return 0
    return sum(
        1
        for c, d in zip(consultora, data_pedido, strict=True)
        if c == HERO_CONSULTORA_ID
        and ((before and d < cutoff) or (after and d >= cutoff))
    )


def _upload_to_volume(
    *,
    local_dir: Path,
    volume_path: str,
) -> int:
    """Upload all parquet files in ``local_dir`` to a UC Volume path.

    Uses the Databricks SDK with the active workspace auth profile. Returns
    the number of files uploaded. Raises if auth or the volume is not
    available — the caller's job to surface that to the user.
    """
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - optional dep
        raise RuntimeError(
            "databricks-sdk is not installed; pip install databricks-sdk to "
            "enable --upload-to"
        ) from exc

    client = WorkspaceClient()
    volume_path = volume_path.rstrip("/")
    count = 0
    for parquet_file in sorted(local_dir.glob("*.parquet")):
        target = f"{volume_path}/{parquet_file.name}"
        with parquet_file.open("rb") as fh:
            client.files.upload(file_path=target, contents=fh, overwrite=True)
        count += 1
    return count


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic Pedido parquet files for the Direct-Sales E2E demo."
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Random seed (default: 42)"
    )
    parser.add_argument(
        "--n-pedidos",
        type=int,
        default=N_PEDIDOS_DEFAULT,
        help=f"Total Pedidos to generate (default: {N_PEDIDOS_DEFAULT})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Local directory for parquet output (created if absent)",
    )
    parser.add_argument(
        "--upload-to",
        type=str,
        default=None,
        help="Optional /Volumes/... destination to upload to after local write",
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
        f"Wrote {len(result.files_written)} parquet files "
        f"({result.n_pedidos} Pedidos) to {result.output_dir}. "
        f"Hero before/after transition: "
        f"{result.hero_pedidos_before_transition}/"
        f"{result.hero_pedidos_after_transition}. "
        f"Planted bad rows: "
        f"{result.n_planted_negative_valor} negative valor + "
        f"{result.n_planted_future_date} future date."
    )

    if args.upload_to:
        n_uploaded = _upload_to_volume(
            local_dir=result.output_dir, volume_path=args.upload_to
        )
        print(f"Uploaded {n_uploaded} parquet files to {args.upload_to}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
