"""Integration test for Slice 07 — report_comissao_pedido_tier_historico MV.

The political-moment MV. Asserts that with Pedidos straddling a known
tier transition, MV2 attributes each pedido's tier_no_momento_do_pedido
to the tier active at data_pedido (not the current tier).

Connects via databricks-connect serverless. Run:

    DATABRICKS_CONFIG_PROFILE=DEFAULT \\
    DATABRICKS_SERVERLESS_COMPUTE_ID=auto \\
    pytest src/tests/test_mv_comissao_historic.py -v

State precondition: hero #42's SCD2 evolution depends on whether bronze
captured both the bronze INSERT and the prata UPDATE. The same
two-shape rule documented on test_autocdc_scd2.py applies here:

  * If hero has 2 SCD2 rows (closed bronze + open prata): pre-transition
    Pedidos must be attributed to 'bronze', post-transition to 'prata'.
  * If hero has 1 SCD2 row (open prata only — current directsales_dev state):
    all of hero's Pedidos are attributed to 'prata' since that's the
    only tier window that ever existed in dim_consultora. The temporal
    join is still mechanism-correct; full evidence requires Slice 12's
    `make reset-sql-server` followed by an end-to-end sqlserver_setup
    Job run.

The MV2 structural and arithmetic ACs (1, 2, 3, 6) gate the suite
regardless of bronze's hero-history state.
"""
from __future__ import annotations

import os
from decimal import Decimal
from typing import Iterator

import pytest

from src.seed.constants import HERO_CONSULTORA_ID, HERO_TIER_AFTER, HERO_TIER_BEFORE


CATALOG = os.environ.get("DEMO_TEST_CATALOG", "directsales_dev")
MV2 = f"{CATALOG}.gold.report_comissao_pedido_tier_historico"

# Mirror of the UDF's CASE branches; centralised so this test catches
# drift between the SQL UDF and the Python view of the rates.
EXPECTED_RATES: dict[str, Decimal] = {
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


@pytest.fixture(scope="module", autouse=True)
def _acl_consultora_full_access(spark) -> Iterator[None]:
    """Slice 08 attached an RLS row filter to gold.dim_consultora. The
    AC 4 test queries dim_consultora directly to branch on hero SCD2
    shape; without an explicit grant for the test runner the count
    returns 0 and the conditional logic silently steers into the
    'single SCD2 row' branch even when full evolution is present.
    Snapshot acl_consultora, grant the runner all 5 regions for the
    module's lifetime, restore on exit.

    No-op if acl_consultora doesn't exist yet (pre-Slice-08 workspaces)."""
    acl = f"{CATALOG}.gold.acl_consultora"
    try:
        spark.sql(f"DESCRIBE TABLE {acl}").collect()
    except Exception:  # pragma: no cover — pre-Slice-08 workspaces
        yield
        return
    user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    snapshot = [
        (r["email"], r["regiao"], r["cpf"])
        for r in spark.sql(f"SELECT email, regiao, cpf FROM {acl}").collect()
    ]
    spark.sql(f"DELETE FROM {acl}")
    grants = [
        (user, r, None)
        for r in ("Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte")
    ]
    spark.createDataFrame(
        grants, "email STRING, regiao STRING, cpf STRING"
    ).writeTo(acl).append()
    try:
        yield
    finally:
        spark.sql(f"DELETE FROM {acl}")
        if snapshot:
            spark.createDataFrame(
                snapshot, "email STRING, regiao STRING, cpf STRING"
            ).writeTo(acl).append()


# ---------- structural ----------


def test_mv2_carries_expected_columns(spark):
    cols = {f.name for f in spark.table(MV2).schema.fields}
    expected = {
        "pedido_id",
        "consultora_id",
        "data_pedido",
        "valor_total",
        "tier_no_momento_do_pedido",
        "comissao_devida",
    }
    assert expected.issubset(cols), f"missing columns: {expected - cols}"


def test_mv2_row_count_at_most_fact_pedido(spark):
    """Each MV2 row corresponds to one Pedido whose data_pedido lies inside
    a SCD2 window for that Consultora. MV2 row count is bounded above by
    fact_pedido — Pedidos whose data_pedido predates the Consultora's
    earliest __START_AT (e.g. seed Consultoras whose registration
    timestamp is later than the start of the 24-month Pedido window) are
    legitimately not attributed."""
    fact = spark.table(f"{CATALOG}.gold.fact_pedido").count()
    mv2 = spark.table(MV2).count()
    assert mv2 <= fact, f"MV2 row count {mv2} > fact_pedido {fact} — duplicate window matches?"
    assert mv2 > 0, "MV2 has zero rows — temporal join produced no matches"


def test_mv2_no_duplicate_pedidos(spark):
    """As-of join must not multiplex a Pedido against multiple SCD2 rows.
    Inclusive lower + exclusive upper makes windows non-overlapping."""
    df = spark.sql(
        f"""
        SELECT pedido_id, COUNT(*) AS n
        FROM {MV2}
        GROUP BY pedido_id
        HAVING COUNT(*) > 1
        """
    )
    assert df.collect() == [], "MV2 has Pedidos joined to multiple SCD2 windows"


# ---------- arithmetic: comissao_devida = valor_total * comissao_pct(tier) ----------


def test_mv2_comissao_devida_matches_valor_times_rate(spark):
    """Spot-check arithmetic on a sample. comissao_devida should equal
    valor_total * the agreed tier rate, regardless of whether the tier
    was historic or current."""
    rows = spark.sql(
        f"""
        SELECT tier_no_momento_do_pedido AS tier,
               valor_total,
               comissao_devida
        FROM {MV2}
        TABLESAMPLE (200 ROWS)
        """
    ).collect()
    assert rows, "MV2 returned no rows for arithmetic check"
    for r in rows:
        rate = EXPECTED_RATES.get(r["tier"])
        assert rate is not None, (
            f"MV2 row has unrecognised tier {r['tier']!r}"
        )
        expected = (r["valor_total"] * rate).quantize(Decimal("0.000001"))
        actual = Decimal(r["comissao_devida"]).quantize(Decimal("0.000001"))
        assert actual == expected, (
            f"comissao_devida mismatch: pedido valor={r['valor_total']} "
            f"tier={r['tier']!r} expected={expected} got={actual}"
        )


# ---------- AC 4: hero #42 historic attribution ----------


def test_hero_42_pedidos_attributed_per_scd2_window(spark):
    """AC 4: Pedidos pre-transition show tier_no_momento = HERO_TIER_BEFORE
    ('bronze'); Pedidos post-transition show HERO_TIER_AFTER ('prata').

    Two valid post-deploy shapes (mirrors test_autocdc_scd2.py):
      A. 2 SCD2 rows for hero -> assert both 'bronze' and 'prata' appear.
      B. 1 SCD2 row -> all of hero's Pedidos attributed to HERO_TIER_AFTER.
    """
    dim_rows = spark.sql(
        f"""
        SELECT COUNT(*) AS n_dim_rows
        FROM {CATALOG}.gold.dim_consultora
        WHERE consultora_id = {HERO_CONSULTORA_ID}
        """
    ).collect()[0]["n_dim_rows"]

    hero_attribution = spark.sql(
        f"""
        SELECT DISTINCT tier_no_momento_do_pedido
        FROM {MV2}
        WHERE consultora_id = {HERO_CONSULTORA_ID}
        """
    ).collect()
    tiers_seen = {r["tier_no_momento_do_pedido"] for r in hero_attribution}

    if dim_rows == 2:
        # Full SCD2 evolution captured. Both tiers must be observed across
        # her Pedidos (12 before + 12 after the transition per
        # constants.HERO_PEDIDOS_TOTAL).
        assert HERO_TIER_BEFORE in tiers_seen, (
            f"hero pre-transition Pedidos missing {HERO_TIER_BEFORE!r}; "
            f"saw tiers {tiers_seen}"
        )
        assert HERO_TIER_AFTER in tiers_seen, (
            f"hero post-transition Pedidos missing {HERO_TIER_AFTER!r}; "
            f"saw tiers {tiers_seen}"
        )
    else:
        # Post-transition snapshot only — see module docstring.
        assert tiers_seen == {HERO_TIER_AFTER}, (
            f"hero with single SCD2 row should attribute all Pedidos to "
            f"{HERO_TIER_AFTER!r}; saw {tiers_seen}"
        )


def test_hero_42_pedidos_pre_transition_use_before_tier(spark):
    """AC 4 (sharper): when hero has 2 SCD2 rows, every Pedido whose
    data_pedido falls before the closed-row __END_AT must use
    HERO_TIER_BEFORE; every Pedido on/after must use HERO_TIER_AFTER.
    Skips when hero has only 1 SCD2 row (post-transition snapshot)."""
    closed_window = spark.sql(
        f"""
        SELECT __END_AT AS end_at
        FROM {CATALOG}.gold.dim_consultora
        WHERE consultora_id = {HERO_CONSULTORA_ID}
          AND __END_AT IS NOT NULL
        """
    ).collect()
    if not closed_window:
        pytest.skip(
            f"hero #{HERO_CONSULTORA_ID} has no closed SCD2 row — "
            f"requires Slice 12 reset path. See module docstring."
        )

    end_at = closed_window[0]["end_at"]
    rows = spark.sql(
        f"""
        SELECT data_pedido, tier_no_momento_do_pedido AS tier
        FROM {MV2}
        WHERE consultora_id = {HERO_CONSULTORA_ID}
        """
    ).collect()
    for r in rows:
        if r["data_pedido"] < end_at:
            assert r["tier"] == HERO_TIER_BEFORE, (
                f"pre-transition Pedido at {r['data_pedido']} "
                f"(< __END_AT={end_at}) attributed to {r['tier']!r}, "
                f"expected {HERO_TIER_BEFORE!r}"
            )
        else:
            assert r["tier"] == HERO_TIER_AFTER, (
                f"post-transition Pedido at {r['data_pedido']} "
                f"(>= __END_AT={end_at}) attributed to {r['tier']!r}, "
                f"expected {HERO_TIER_AFTER!r}"
            )
