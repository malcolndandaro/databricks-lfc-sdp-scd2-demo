"""Synthetic Consultora SQL generator — sibling of generate_pedidos_parquet.

Emits a deterministic ``sql_server/ddl/03_seed_data.sql`` file with 500
``INSERT`` statements that conform to the PRD distributions:

* tier: 40 % semente, 30 % bronze, 18 % prata, 10 % ouro, 2 % diamante
* regiao: even-ish across the 5 Brazilian regions
* CPF: plausible 11-digit Brazilian format, one row intentionally malformed
  for the silver Expectations beat (Slice 05)
* Hero ``consultora_id = 42`` is seeded with ``tier = HERO_TIER_BEFORE`` (bronze);
  Slice 02's ``04_apply_tier_transitions.sql`` will UPDATE her to
  ``HERO_TIER_AFTER`` (prata) with ``updated_at = HERO_TRANSITION_DATE``,
  producing a real Change Tracking event for Lakeflow Connect to stream.

Same seed → identical SQL output. Re-run by:

::

   python -m src.seed.generate_consultoras_sql
"""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Sequence

from faker import Faker

from src.seed.constants import (
    HERO_CONSULTORA_ID,
    HERO_TIER_BEFORE,
    HERO_TRANSITION_DATE,
    N_CONSULTORAS,
    REGIONS,
    TIER_DISTRIBUTION,
)


def _generate_cpf(rng: random.Random) -> str:
    """Generate a plausible 11-digit Brazilian CPF with valid check digits.

    Format: 11 digits, no separators (matches PRD column type ``VARCHAR(11)``).
    Implements the standard CPF check-digit algorithm so values look real
    even though they're synthetic.
    """
    base = [rng.randint(0, 9) for _ in range(9)]

    s1 = sum(b * w for b, w in zip(base, range(10, 1, -1)))
    d1 = 0 if s1 % 11 < 2 else 11 - (s1 % 11)

    base_with_d1 = base + [d1]
    s2 = sum(b * w for b, w in zip(base_with_d1, range(11, 1, -1)))
    d2 = 0 if s2 % 11 < 2 else 11 - (s2 % 11)

    return "".join(str(d) for d in base + [d1, d2])


def _build_tiers(n: int, rng: random.Random) -> list[str]:
    """Build a tier list of length ``n`` matching the PRD distribution exactly.

    Counts are computed deterministically from N_CONSULTORAS and shuffled with
    the seeded RNG so the order is stable across re-runs.
    """
    counts = {tier: int(round(p * n)) for tier, p in TIER_DISTRIBUTION.items()}
    drift = n - sum(counts.values())
    if drift != 0:
        counts["semente"] += drift  # absorb rounding in the largest bucket
    tiers: list[str] = []
    for tier, count in counts.items():
        tiers.extend([tier] * count)
    rng.shuffle(tiers)
    return tiers


def _sql_escape(value: str) -> str:
    """Escape single quotes for T-SQL string literals."""
    return value.replace("'", "''")


def _format_insert(
    *,
    consultora_id: int,
    cpf: str,
    nome: str,
    email: str | None,
    regiao: str,
    tier: str,
    data_cadastro: datetime,
    ativo: bool,
    updated_at: datetime,
) -> str:
    """Render a single ``INSERT`` row as one SQL statement.

    Uses ``DATETIME2`` literal format. ``email`` may be NULL.
    """
    email_sql = "NULL" if email is None else f"N'{_sql_escape(email)}'"
    return (
        "INSERT INTO {{schema}}.consultoras "
        "(consultora_id, cpf, nome, email, regiao, tier, data_cadastro, ativo, updated_at) "
        "VALUES ("
        f"{consultora_id}, "
        f"'{cpf}', "
        f"N'{_sql_escape(nome)}', "
        f"{email_sql}, "
        f"'{regiao}', "
        f"'{tier}', "
        f"'{data_cadastro.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', "
        f"{1 if ativo else 0}, "
        f"'{updated_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        ");"
    )


def generate_sql(
    *,
    seed: int = 42,
    n_consultoras: int = N_CONSULTORAS,
    output_path: Path | str,
) -> Path:
    """Generate the ``03_seed_data.sql`` file deterministically.

    Args:
        seed: Random seed (numpy/random/Faker all initialised from this).
        n_consultoras: Pool size — must be at least HERO_CONSULTORA_ID.
        output_path: File path to write SQL to.

    Returns:
        The output ``Path``.
    """
    if n_consultoras < HERO_CONSULTORA_ID:
        raise ValueError(
            f"n_consultoras={n_consultoras} cannot be smaller than "
            f"HERO_CONSULTORA_ID={HERO_CONSULTORA_ID}"
        )

    rng = random.Random(seed)
    faker = Faker("pt_BR")
    faker.seed_instance(seed)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tiers = _build_tiers(n_consultoras, rng)

    # Hero #42 is forced to HERO_TIER_BEFORE so that 04_apply_tier_transitions.sql
    # can produce the bronze->prata transition Change Tracking event.
    tiers[HERO_CONSULTORA_ID - 1] = HERO_TIER_BEFORE

    # data_cadastro spread over a 3-year history ending today; deterministic.
    today = datetime(2026, 5, 8)
    cadastro_window_days = 365 * 3

    # Plant exactly one malformed CPF for the silver Expectations beat. Pick a
    # consultora that is NOT the hero so we don't entangle two demo concerns.
    malformed_consultora_id = 7  # arbitrary, not hero

    rows: list[str] = []
    for cid in range(1, n_consultoras + 1):
        tier = tiers[cid - 1]
        regiao = REGIONS[(cid - 1) % len(REGIONS)]
        nome = faker.name()
        email_local = (
            nome.split()[0].lower().replace("ã", "a").replace("á", "a")
            .replace("é", "e").replace("í", "i").replace("ó", "o")
            .replace("ç", "c")
        )
        email = f"{email_local}.{cid}@example.com.br"
        if cid == malformed_consultora_id:
            cpf = "ABC123XX99"  # 10 chars + letters → fails RLIKE '^[0-9]{11}$'
        else:
            cpf = _generate_cpf(rng)

        if cid == HERO_CONSULTORA_ID:
            # Hero must have been cadastrada well before her tier transition so
            # her dim_consultora row's __START_AT predates the bronze→prata edge.
            data_cadastro = datetime(2024, 1, 15)
        elif cid != malformed_consultora_id and cid % 10 == 0:
            # Tier-transition candidates (50 rows; ids 10, 20, …, 500 minus
            # hero + malformed). Pinned to a deterministic pre-transition
            # data_cadastro so 04_apply_tier_transitions.sql's idempotency
            # guard (`updated_at < '2025-11-01'`) catches all 50 on first
            # run and zero on every re-run.
            data_cadastro = datetime(2025, 9, 1)
        else:
            data_cadastro = today - timedelta(
                days=rng.randint(0, cadastro_window_days)
            )
        # updated_at == data_cadastro for the seed; tier transitions later override it.
        updated_at = data_cadastro
        ativo = rng.random() < 0.97  # 3% inactive — adds modest variance

        rows.append(
            _format_insert(
                consultora_id=cid,
                cpf=cpf,
                nome=nome,
                email=email,
                regiao=regiao,
                tier=tier,
                data_cadastro=data_cadastro,
                ativo=ativo,
                updated_at=updated_at,
            )
        )

    header = [
        "-- 03_seed_data.sql",
        "--",
        "-- Generated by src/seed/generate_consultoras_sql.py — DO NOT EDIT BY HAND.",
        f"-- {n_consultoras} Consultoras with PRD-compliant distributions.",
        f"-- Hero consultora_id = {HERO_CONSULTORA_ID} seeded as tier = {HERO_TIER_BEFORE!r};",
        "-- 04_apply_tier_transitions.sql will UPDATE her to the post-transition tier.",
        f"-- Exactly one Consultora (id={malformed_consultora_id}) carries a malformed CPF for the",
        "-- silver Expectations beat (Slice 05).",
        "--",
        "-- Idempotent: every INSERT is wrapped in a NOT EXISTS guard.",
        "",
        "USE DemoDB;",
        "GO",
        "",
    ]

    insert_blocks: list[str] = []
    for cid, row in zip(range(1, n_consultoras + 1), rows, strict=True):
        insert_blocks.append(
            f"IF NOT EXISTS (SELECT 1 FROM {{{{schema}}}}.consultoras WHERE consultora_id = {cid})\n"
            f"    {row}"
        )

    body = "\n".join(insert_blocks)
    output_path.write_text(
        "\n".join(header) + body + "\n\nGO\n", encoding="utf-8"
    )
    return output_path


def main(argv: Sequence[str] | None = None) -> int:
    output = Path("sql_server/ddl/03_seed_data.sql")
    written = generate_sql(seed=42, output_path=output)
    print(f"Wrote {written} ({N_CONSULTORAS} Consultoras).")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
