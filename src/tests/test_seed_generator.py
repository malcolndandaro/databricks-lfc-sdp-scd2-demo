"""Tests for src.seed.generate_pedidos_parquet.

Covers the AC for Slice 03: determinism, distributions within ±5 % of
targets, hero #42 straddles the transition, and planted bad rows are present
in the agreed counts.

Tests use small ``n_pedidos`` (5 000) to stay fast; distributions still hit
±5 % at this scale.
"""
from __future__ import annotations

import hashlib
from collections import Counter
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.seed.constants import (
    BAD_FUTURE_DATE_COUNT,
    BAD_FUTURE_YEAR,
    BAD_NEGATIVE_VALOR_COUNT,
    FORMA_PAGAMENTO_DISTRIBUTION,
    HERO_CONSULTORA_ID,
    HERO_PEDIDOS_TOTAL,
    HERO_TRANSITION_DATE,
    N_CONSULTORAS,
    STATUS_DISTRIBUTION,
)
from src.seed.generate_pedidos_parquet import generate

TODAY = datetime(2026, 5, 8)
N_PEDIDOS = 5_000
TOLERANCE = 0.05


@pytest.fixture(scope="module")
def generated_once(tmp_path_factory: pytest.TempPathFactory):
    """Generate once per module — tests below all read from the same files."""
    output = tmp_path_factory.mktemp("seed_run")
    result = generate(
        seed=42,
        n_pedidos=N_PEDIDOS,
        output_dir=output,
        today=TODAY,
        pedidos_per_file=500,
    )
    return result


@pytest.fixture(scope="module")
def loaded_table(generated_once):
    """Concat the parquet files back into a single Arrow table for assertion."""
    paths = sorted(generated_once.output_dir.glob("*.parquet"))
    tables = [pq.read_table(p) for p in paths]
    import pyarrow as pa

    return pa.concat_tables(tables)


# ---------- determinism ----------


def test_determinism_same_seed_same_bytes(tmp_path: Path):
    """Same seed + same `today` → identical parquet bytes."""
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    generate(seed=99, n_pedidos=2_000, output_dir=out_a, today=TODAY, pedidos_per_file=500)
    generate(seed=99, n_pedidos=2_000, output_dir=out_b, today=TODAY, pedidos_per_file=500)

    files_a = sorted(out_a.glob("*.parquet"))
    files_b = sorted(out_b.glob("*.parquet"))
    assert [p.name for p in files_a] == [p.name for p in files_b]
    for fa, fb in zip(files_a, files_b, strict=True):
        assert hashlib.sha256(fa.read_bytes()).hexdigest() == hashlib.sha256(
            fb.read_bytes()
        ).hexdigest(), f"non-deterministic output: {fa.name}"


def test_different_seed_different_output(tmp_path: Path):
    """Sanity: different seed produces a different first file."""
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    generate(seed=1, n_pedidos=1_000, output_dir=out_a, today=TODAY, pedidos_per_file=500)
    generate(seed=2, n_pedidos=1_000, output_dir=out_b, today=TODAY, pedidos_per_file=500)
    first_a = (out_a / "pedidos_0000.parquet").read_bytes()
    first_b = (out_b / "pedidos_0000.parquet").read_bytes()
    assert hashlib.sha256(first_a).hexdigest() != hashlib.sha256(first_b).hexdigest()


# ---------- shape & cardinality ----------


def test_total_row_count(generated_once):
    assert generated_once.n_pedidos == N_PEDIDOS


def test_files_split_correctly(generated_once):
    """At pedidos_per_file=500 with N_PEDIDOS=5000 we expect exactly 10 files."""
    assert len(generated_once.files_written) == N_PEDIDOS // 500


def test_consultora_ids_in_range(loaded_table):
    ids = set(loaded_table["consultora_id"].to_pylist())
    assert min(ids) >= 1
    assert max(ids) <= N_CONSULTORAS


# ---------- distributions (±5 %) ----------


def _within_tolerance(actual: float, expected: float, tol: float = TOLERANCE) -> bool:
    return abs(actual - expected) <= tol


def test_status_distribution(loaded_table):
    counts = Counter(loaded_table["status"].to_pylist())
    for status, expected_p in STATUS_DISTRIBUTION.items():
        actual_p = counts.get(status, 0) / N_PEDIDOS
        assert _within_tolerance(actual_p, expected_p), (
            f"status={status}: actual {actual_p:.3f} vs expected {expected_p:.3f}"
        )


def test_forma_pagamento_distribution(loaded_table):
    counts = Counter(loaded_table["forma_pagamento"].to_pylist())
    for pgto, expected_p in FORMA_PAGAMENTO_DISTRIBUTION.items():
        actual_p = counts.get(pgto, 0) / N_PEDIDOS
        assert _within_tolerance(actual_p, expected_p), (
            f"forma_pagamento={pgto}: actual {actual_p:.3f} vs expected {expected_p:.3f}"
        )


def test_valor_total_lognormal_shape(loaded_table):
    """Center should be near R$ 250 (median of lognormal ≈ exp(mu) ≈ 244).
    Use the median, not the mean — lognormal mean is pulled by the tail.
    """
    valores = [v for v in loaded_table["valor_total"].to_pylist() if v > 0]
    valores_sorted = sorted(valores)
    median = valores_sorted[len(valores_sorted) // 2]
    assert 200 <= median <= 300, f"median {median} not in [200, 300]"
    # Tail should reach above R$ 1k for at least some Pedidos.
    assert max(valores) > 1_000


# ---------- hero Consultora #42 ----------


def test_hero_pedidos_count_at_least_target(generated_once):
    """Hero #42 has at least HERO_PEDIDOS_TOTAL Pedidos.

    The deterministic hero block contributes exactly HERO_PEDIDOS_TOTAL; the
    random draw will also land on consultora_id=42 occasionally. The contract
    is "at least the deterministic count", not "exactly".
    """
    total = (
        generated_once.hero_pedidos_before_transition
        + generated_once.hero_pedidos_after_transition
    )
    assert total >= HERO_PEDIDOS_TOTAL


def test_hero_pedidos_straddle_transition(generated_once):
    """Hero #42's deterministic Pedidos straddle the transition date.

    Lower-bounded by the deterministic half-count; random draws may add to
    either side but cannot subtract.
    """
    assert generated_once.hero_pedidos_before_transition >= HERO_PEDIDOS_TOTAL // 2
    assert generated_once.hero_pedidos_after_transition >= HERO_PEDIDOS_TOTAL // 2


def test_hero_pedidos_dates_around_transition(loaded_table):
    """Cross-check: load the table, filter by hero, assert both halves are
    populated. We don't assert exact counts because random draws on
    consultora_id=42 inflate the total — the contract is "Pedidos straddle",
    not "exact count"."""
    consultora = loaded_table["consultora_id"].to_pylist()
    data_pedido = loaded_table["data_pedido"].to_pylist()
    transition_dt = datetime(
        HERO_TRANSITION_DATE.year,
        HERO_TRANSITION_DATE.month,
        HERO_TRANSITION_DATE.day,
    )
    hero_dates = [d for c, d in zip(consultora, data_pedido) if c == HERO_CONSULTORA_ID]
    assert len(hero_dates) >= HERO_PEDIDOS_TOTAL
    before = [d for d in hero_dates if d < transition_dt]
    after = [d for d in hero_dates if d >= transition_dt]
    assert len(before) >= HERO_PEDIDOS_TOTAL // 2
    assert len(after) >= HERO_PEDIDOS_TOTAL // 2


# ---------- planted bad rows ----------


def test_planted_negative_valor_count(generated_once):
    assert generated_once.n_planted_negative_valor == BAD_NEGATIVE_VALOR_COUNT


def test_planted_future_date_count(generated_once):
    assert generated_once.n_planted_future_date == BAD_FUTURE_DATE_COUNT


def test_planted_future_dates_are_in_target_year(loaded_table):
    data_pedido = loaded_table["data_pedido"].to_pylist()
    future_dates = [d for d in data_pedido if d.year == BAD_FUTURE_YEAR]
    assert len(future_dates) == BAD_FUTURE_DATE_COUNT


def test_planted_negative_valor_is_exactly_negative_one(loaded_table):
    """Silver Expectation `valor_positivo` checks ``valor_total > 0`` —
    the planted value is exactly ``-1.0`` so the violation count is precise."""
    valores = loaded_table["valor_total"].to_pylist()
    negative = [v for v in valores if v <= 0]
    assert len(negative) == BAD_NEGATIVE_VALOR_COUNT
    assert all(v == -1.0 for v in negative)


# ---------- schema integrity ----------


def test_schema_has_expected_columns(loaded_table):
    expected = {
        "pedido_id",
        "consultora_id",
        "data_pedido",
        "valor_total",
        "status",
        "forma_pagamento",
        "updated_at",
    }
    assert set(loaded_table.column_names) == expected


def test_pedido_ids_are_unique(loaded_table):
    ids = loaded_table["pedido_id"].to_pylist()
    assert len(ids) == len(set(ids)), "pedido_id duplicates detected"


# ---------- input validation ----------


def test_rejects_zero_n_pedidos(tmp_path: Path):
    with pytest.raises(ValueError, match="n_pedidos"):
        generate(seed=42, n_pedidos=0, output_dir=tmp_path, today=TODAY)


def test_rejects_n_consultoras_below_hero_id(tmp_path: Path):
    with pytest.raises(ValueError, match="HERO_CONSULTORA_ID"):
        generate(
            seed=42,
            n_pedidos=1_000,
            output_dir=tmp_path,
            today=TODAY,
            n_consultoras=10,
        )
