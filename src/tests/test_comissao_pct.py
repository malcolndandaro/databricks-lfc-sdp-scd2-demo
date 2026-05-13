"""Integration test for Slice 07 — gold.comissao_pct UDF.

Asserts the agreed tier -> commission rate mapping. The UDF is the single
declarative source of truth used by both fact_pedido derivations and
report_comissao_pedido_tier_historico, so any drift here corrupts the
demo's headline as-of-comissao MV.

Connects via databricks-connect serverless (project test convention). Run:

    DATABRICKS_CONFIG_PROFILE=DEFAULT \\
    DATABRICKS_SERVERLESS_COMPUTE_ID=auto \\
    pytest src/tests/test_comissao_pct.py -v

Design choice on unknown tier: returns NULL (per the SQL UDF's CASE/ELSE
branch). The alternative — RAISE_ERROR — would surface drift loudly in
the SDP Quality tab but also break the comissao MV when even one
unrecognised tier shows up. NULL keeps the rollup working and surfaces
the bad row downstream.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Iterator

import pytest


CATALOG = os.environ.get("DEMO_TEST_CATALOG", "directsales_dev")

# Single source of truth for tier rates — must match the CASE in
# src/sdp/03_gold_reports.sql exactly.
TIER_RATES: dict[str, Decimal] = {
    "semente": Decimal("0.0200"),
    "bronze": Decimal("0.0300"),
    "prata": Decimal("0.0500"),
    "ouro": Decimal("0.0800"),
    "diamante": Decimal("0.1200"),
}


def _spark():
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.getOrCreate()


@pytest.fixture(scope="module")
def spark() -> Iterator:
    try:
        s = _spark()
    except Exception as exc:  # pragma: no cover — env-dependent
        pytest.skip(f"databricks-connect unavailable: {exc}")
    yield s


@pytest.mark.parametrize("tier,expected", list(TIER_RATES.items()))
def test_comissao_pct_per_tier(spark, tier: str, expected: Decimal):
    """Each of the 5 documented tiers returns its agreed rate."""
    row = spark.sql(
        f"SELECT {CATALOG}.gold.comissao_pct('{tier}') AS rate"
    ).collect()[0]
    assert row["rate"] == expected, (
        f"comissao_pct('{tier}') = {row['rate']!r}, expected {expected!r}"
    )


def test_comissao_pct_unknown_tier_returns_null(spark):
    """Unknown tier returns NULL (documented design choice — see module docstring)."""
    row = spark.sql(
        f"SELECT {CATALOG}.gold.comissao_pct('platina_invalida') AS rate"
    ).collect()[0]
    assert row["rate"] is None, (
        f"unknown tier should return NULL, got {row['rate']!r}"
    )


def test_comissao_pct_null_input_returns_null(spark):
    """NULL input returns NULL (CASE WHEN tier matches no branch)."""
    row = spark.sql(
        f"SELECT {CATALOG}.gold.comissao_pct(CAST(NULL AS STRING)) AS rate"
    ).collect()[0]
    assert row["rate"] is None


def test_comissao_pct_return_type_is_decimal_5_4(spark):
    """Return type is DECIMAL(5,4) — affects DECIMAL arithmetic in MV2."""
    schema = spark.sql(
        f"SELECT {CATALOG}.gold.comissao_pct('prata') AS rate"
    ).schema
    rate_field = next(f for f in schema.fields if f.name == "rate")
    type_str = rate_field.dataType.simpleString()
    assert type_str == "decimal(5,4)", (
        f"rate column type {type_str!r}, expected decimal(5,4)"
    )


def test_comissao_pct_rates_are_strictly_increasing_with_tier(spark):
    """Higher tier -> higher commission. Catches accidentally-swapped CASE branches."""
    tiers_ascending = ["semente", "bronze", "prata", "ouro", "diamante"]
    rates = [
        spark.sql(
            f"SELECT {CATALOG}.gold.comissao_pct('{t}') AS rate"
        ).collect()[0]["rate"]
        for t in tiers_ascending
    ]
    for i in range(len(rates) - 1):
        assert rates[i] < rates[i + 1], (
            f"comissao not strictly increasing at "
            f"{tiers_ascending[i]} ({rates[i]}) -> "
            f"{tiers_ascending[i + 1]} ({rates[i + 1]})"
        )
