"""Tests for src.seed.generate_canal_origem_pedidos.

Covers Slice 09: a small parquet generator that adds a NEW `canal_origem`
column for the schema-evolution live beat. Tests pin the schema, distribution,
determinism, and that pedido_id stays clear of the main 1..50_000 range.
"""
from __future__ import annotations

import hashlib
from collections import Counter
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.seed.constants import N_CONSULTORAS
from src.seed.generate_canal_origem_pedidos import (
    CANAL_ORIGEM_DISTRIBUTION,
    DEFAULT_PEDIDO_ID_START,
    filename_for,
    generate,
)

TODAY = datetime(2026, 5, 8)


def test_filename_includes_date_and_seed():
    """Filenames must encode date + seed so successive demo drops never collide."""
    assert (
        filename_for(seed=909, today=TODAY)
        == "pedidos_canal_origem_20260508_000909.parquet"
    )
    assert (
        filename_for(seed=1, today=datetime(2026, 1, 1))
        == "pedidos_canal_origem_20260101_000001.parquet"
    )


def test_generate_default_size_and_path(tmp_path: Path):
    result = generate(seed=909, output_dir=tmp_path, today=TODAY)
    assert result.n_pedidos == 100
    assert result.output_path.exists()
    assert result.output_path.name.startswith("pedidos_canal_origem_20260508_")
    assert result.output_path.suffix == ".parquet"


def test_generate_writes_exactly_one_file(tmp_path: Path):
    """One parquet file per call — the demo drop is a single-file event."""
    generate(seed=909, output_dir=tmp_path, today=TODAY)
    files = list(tmp_path.glob("*.parquet"))
    assert len(files) == 1


def test_schema_includes_canal_origem(tmp_path: Path):
    """Schema must match bronze.pedidos_raw's source columns plus canal_origem."""
    result = generate(seed=909, output_dir=tmp_path, today=TODAY)
    schema = pq.read_schema(result.output_path)
    fields = {f.name: f.type for f in schema}

    assert "canal_origem" in fields, "canal_origem must be present"
    assert str(fields["canal_origem"]) == "string"

    # Pin the rest so a future tweak to the main generator can't drift this one.
    expected = {
        "pedido_id": "int64",
        "consultora_id": "int32",
        "data_pedido": "timestamp[us]",
        "valor_total": "double",
        "status": "string",
        "forma_pagamento": "string",
        "updated_at": "timestamp[us]",
        "canal_origem": "string",
    }
    for col, expected_type in expected.items():
        assert col in fields, f"missing column: {col}"
        assert (
            str(fields[col]) == expected_type
        ), f"{col}: expected {expected_type}, got {fields[col]}"


def test_canal_distribution_within_tolerance(tmp_path: Path):
    """60 % app / 30 % web / 10 % whatsapp within ±15 % at n=100.

    100 rows is a small sample so we use a generous tolerance — the strong
    correctness guarantee here is that ONLY the three valid values appear.
    """
    result = generate(seed=909, output_dir=tmp_path, today=TODAY, n_pedidos=100)
    table = pq.read_table(result.output_path)
    canais = table["canal_origem"].to_pylist()

    counts = Counter(canais)
    assert set(counts.keys()) == set(CANAL_ORIGEM_DISTRIBUTION.keys()), (
        f"unexpected canal values: {set(counts.keys())}"
    )

    n = len(canais)
    for canal, target_share in CANAL_ORIGEM_DISTRIBUTION.items():
        observed = counts[canal] / n
        assert abs(observed - target_share) <= 0.15, (
            f"{canal}: observed={observed:.2f}, target={target_share}"
        )


def test_canal_counts_match_returned_dict(tmp_path: Path):
    """Returned canal_counts must match the parquet contents."""
    result = generate(seed=909, output_dir=tmp_path, today=TODAY)
    table = pq.read_table(result.output_path)
    actual = Counter(table["canal_origem"].to_pylist())
    assert dict(actual) == result.canal_counts


def test_pedido_id_does_not_collide_with_main_range(tmp_path: Path):
    """pedido_ids start at 1_000_000 by default — never overlap the 50k main set."""
    result = generate(seed=909, output_dir=tmp_path, today=TODAY)
    table = pq.read_table(result.output_path)
    ids = table["pedido_id"].to_pylist()
    assert min(ids) >= DEFAULT_PEDIDO_ID_START
    assert max(ids) < DEFAULT_PEDIDO_ID_START + len(ids)
    assert len(set(ids)) == len(ids), "pedido_ids must be unique within a drop"


def test_consultora_ids_within_pool(tmp_path: Path):
    """consultora_id stays in 1..N_CONSULTORAS so FKs to silver.consultoras hold."""
    result = generate(seed=909, output_dir=tmp_path, today=TODAY)
    table = pq.read_table(result.output_path)
    ids = table["consultora_id"].to_pylist()
    assert min(ids) >= 1
    assert max(ids) <= N_CONSULTORAS


def test_no_planted_bad_rows(tmp_path: Path):
    """canal_origem files are CLEAN — no negative valor / future-date plants.

    The bad-row plants live in the main 50k generator (Slice 03). Adding them
    here would leak into the live beat and confuse the silver Expectations
    counters mid-demo.
    """
    result = generate(seed=909, output_dir=tmp_path, today=TODAY)
    table = pq.read_table(result.output_path)
    valores = table["valor_total"].to_pylist()
    datas = table["data_pedido"].to_pylist()
    assert all(v > 0 for v in valores), "no negative valor_total expected"
    assert all(d.year < 2099 for d in datas), "no year-2099 dates expected"


def test_determinism_same_seed_same_bytes(tmp_path: Path):
    """Same seed + same `today` -> identical parquet bytes."""
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    a = generate(seed=909, output_dir=out_a, today=TODAY)
    b = generate(seed=909, output_dir=out_b, today=TODAY)
    assert a.output_path.name == b.output_path.name
    sha_a = hashlib.sha256(a.output_path.read_bytes()).hexdigest()
    sha_b = hashlib.sha256(b.output_path.read_bytes()).hexdigest()
    assert sha_a == sha_b, "non-deterministic output"


def test_different_seed_different_bytes(tmp_path: Path):
    """Different seeds -> different content (so demo drops don't repeat)."""
    out_a = tmp_path / "a"
    out_b = tmp_path / "b"
    a = generate(seed=909, output_dir=out_a, today=TODAY)
    b = generate(seed=910, output_dir=out_b, today=TODAY)
    assert a.output_path.name != b.output_path.name


def test_n_pedidos_must_be_positive(tmp_path: Path):
    with pytest.raises(ValueError, match="n_pedidos must be positive"):
        generate(seed=909, n_pedidos=0, output_dir=tmp_path, today=TODAY)
