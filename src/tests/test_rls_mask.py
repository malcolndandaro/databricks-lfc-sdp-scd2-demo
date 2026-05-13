"""Integration test for Slice 08 — gold.acl_consultora + RLS + column mask.

Asserts behaviour of:
  * gold.consultora_rls(regiao STRING) -> BOOLEAN
  * gold.mask_cpf(cpf STRING) -> STRING
  * gold.dim_consultora when row filter + column mask are attached.

Connects via databricks-connect serverless. Run:

    DATABRICKS_CONFIG_PROFILE=DEFAULT \\
    DATABRICKS_SERVERLESS_COMPUTE_ID=auto \\
    pytest src/tests/test_rls_mask.py -v

Why we don't impersonate users
------------------------------

The PRD AC text talks about "parametrised current_user() values", but
databricks-connect serverless runs as a single fixed identity (the test
runner — Malcoln). We cannot SET CURRENT_USER. The substitute pattern
this test uses:

  * Snapshot acl_consultora state at module entry, restore at module exit.
  * Within tests, INSERT / DELETE acl_consultora rows for the actual
    current_user() (the test runner) and assert the UDFs flip
    behaviour as a function of which acl_consultora rows match
    current_user().
  * Never assert behaviour for hypothetical other users — those checks
    exist mechanically because the UDF body uses current_user(); if
    they work for the runner they work for everyone (the demo proves
    the user-difference live with two browser tabs, not in pytest).

Owner-bypass note
-----------------

If the test runner happens to own the catalog/schema/table, UC may
exempt them from row filters and column masks. The bundle's reset
path (Slice 12) deliberately does NOT change ownership, so the demo
state matches the test state — Malcoln is bound by RLS the same way
the audience sees it on stage. If the end-to-end SELECT tests show
ownership-bypass, that's a workspace configuration issue and the
demo's governance scene won't land either.

State precondition
------------------

This test requires:
  * `governance_setup` Job has run (acl_consultora table exists; UDFs
    created; ALTER TABLE attached the row filter + column mask to
    gold.dim_consultora).
  * SDP pipeline has run at least once (gold.dim_consultora is
    populated).
"""
from __future__ import annotations

import os
from typing import Iterator

import pytest


CATALOG = os.environ.get("DEMO_TEST_CATALOG", "directsales_dev")
ACL = f"{CATALOG}.gold.acl_consultora"
DIM = f"{CATALOG}.gold.dim_consultora"
RLS_FN = f"{CATALOG}.gold.consultora_rls"
MASK_FN = f"{CATALOG}.gold.mask_cpf"

MASKED_GLYPH = "***.***.***-**"

# Synthetic admin row used to baseline acl_consultora when the test
# clears it. Matches the shape Slice 12's reset_databricks.py will
# seed; keeping it here means the test does not depend on Slice 12
# having shipped.
SEED_ADMIN_EMAIL = "admin@demo.invalid"


def _spark():
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.getOrCreate()


@pytest.fixture(scope="module")
def spark() -> Iterator:
    try:
        s = _spark()
    except Exception as exc:  # pragma: no cover — env-dependent
        pytest.skip(f"databricks-connect unavailable: {exc}")
    # Pre-flight: skip the suite if the governance assets aren't deployed
    # yet. Better than letting every test fail with TABLE_OR_VIEW_NOT_FOUND.
    try:
        s.sql(f"DESCRIBE TABLE {ACL}").collect()
    except Exception as exc:  # pragma: no cover — env-dependent
        pytest.skip(
            f"{ACL} not deployed — run `databricks bundle run governance_setup -t dev` first ({exc})"
        )
    yield s


@pytest.fixture(scope="module")
def current_user(spark) -> str:
    return spark.sql("SELECT current_user() AS u").collect()[0]["u"]


@pytest.fixture(autouse=True)
def acl_state_isolation(spark):
    """Snapshot acl_consultora before the test, restore after.

    Each test gets a clean acl_consultora with exactly the rows it inserts.
    This keeps tests independent and avoids leaving stray grants between
    rehearsals if the test crashes mid-run."""
    rows = spark.sql(f"SELECT email, regiao, cpf FROM {ACL}").collect()
    snapshot = [(r["email"], r["regiao"], r["cpf"]) for r in rows]
    spark.sql(f"DELETE FROM {ACL}")
    try:
        yield
    finally:
        spark.sql(f"DELETE FROM {ACL}")
        if snapshot:
            df = spark.createDataFrame(snapshot, "email STRING, regiao STRING, cpf STRING")
            df.writeTo(ACL).append()


def _insert_acl(spark, email: str, regiao: str | None, cpf: str | None):
    """Insert a single (email, regiao, cpf) row. Uses parametrised SQL
    via createDataFrame to avoid SQL-injection issues on email values."""
    df = spark.createDataFrame(
        [(email, regiao, cpf)], "email STRING, regiao STRING, cpf STRING"
    )
    df.writeTo(ACL).append()


# ---------- acl_consultora schema + bundle-resource AC ----------


def test_acl_consultora_schema_matches_contract(spark):
    """AC: acl_consultora has exactly columns email STRING, regiao STRING, cpf STRING."""
    schema = {f.name: f.dataType.simpleString() for f in spark.table(ACL).schema.fields}
    assert schema == {"email": "string", "regiao": "string", "cpf": "string"}, (
        f"acl_consultora schema drift: {schema}"
    )


# ---------- consultora_rls UDF ----------


def test_rls_returns_false_when_no_grant_for_current_user(spark, current_user):
    """With acl_consultora empty for current_user(), the row filter rejects every region."""
    _insert_acl(spark, SEED_ADMIN_EMAIL, "Sudeste", None)
    for regiao in ("Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"):
        result = spark.sql(f"SELECT {RLS_FN}('{regiao}') AS r").collect()[0]["r"]
        assert result is False, (
            f"consultora_rls('{regiao}') = {result} — expected False since "
            f"acl_consultora has no row for {current_user}"
        )


def test_rls_returns_true_only_for_granted_region(spark, current_user):
    """With one grant for (current_user(), 'Sudeste'), only Sudeste passes."""
    _insert_acl(spark, current_user, "Sudeste", None)
    for regiao, expected in [
        ("Sudeste", True),
        ("Sul", False),
        ("Nordeste", False),
        ("Centro-Oeste", False),
        ("Norte", False),
    ]:
        result = spark.sql(f"SELECT {RLS_FN}('{regiao}') AS r").collect()[0]["r"]
        assert result is expected, (
            f"consultora_rls('{regiao}') = {result}, expected {expected}"
        )


def test_rls_returns_true_for_multiple_granted_regions(spark, current_user):
    """Multiple grants for the same user are independent."""
    _insert_acl(spark, current_user, "Sudeste", None)
    _insert_acl(spark, current_user, "Norte", None)
    for regiao, expected in [
        ("Sudeste", True),
        ("Norte", True),
        ("Sul", False),
    ]:
        result = spark.sql(f"SELECT {RLS_FN}('{regiao}') AS r").collect()[0]["r"]
        assert result is expected, (
            f"consultora_rls('{regiao}') = {result}, expected {expected}"
        )


# ---------- mask_cpf UDF ----------


def test_mask_returns_glyph_when_no_grant(spark, current_user):
    """With no acl_consultora row for current_user(), every cpf is masked."""
    _insert_acl(spark, SEED_ADMIN_EMAIL, "Sudeste", "12345678901")
    for cpf in ("12345678901", "98765432100", "00000000000"):
        result = spark.sql(f"SELECT {MASK_FN}('{cpf}') AS m").collect()[0]["m"]
        assert result == MASKED_GLYPH, (
            f"mask_cpf('{cpf}') = {result!r}, expected {MASKED_GLYPH!r} "
            f"since {current_user} has no grant"
        )


def test_mask_returns_raw_when_user_has_specific_cpf_grant(spark, current_user):
    """With (current_user(), Sudeste, '12345678901') in acl_consultora,
    only that exact cpf round-trips raw; others stay masked."""
    _insert_acl(spark, current_user, "Sudeste", "12345678901")
    raw = spark.sql(f"SELECT {MASK_FN}('12345678901') AS m").collect()[0]["m"]
    assert raw == "12345678901", (
        f"mask_cpf for granted cpf returned {raw!r}, expected raw"
    )
    masked = spark.sql(f"SELECT {MASK_FN}('99999999999') AS m").collect()[0]["m"]
    assert masked == MASKED_GLYPH, (
        f"mask_cpf for non-granted cpf returned {masked!r}, expected {MASKED_GLYPH!r}"
    )


def test_mask_returns_glyph_when_user_grant_has_null_cpf(spark, current_user):
    """A region grant with NULL cpf gives row-visibility but NOT cpf
    unmasking. Mirrors PRD AC 7 ('returns Sudeste rows with cpf masked
    as ***.***.***-**')."""
    _insert_acl(spark, current_user, "Sudeste", None)
    for cpf in ("12345678901", "98765432100"):
        result = spark.sql(f"SELECT {MASK_FN}('{cpf}') AS m").collect()[0]["m"]
        assert result == MASKED_GLYPH, (
            f"mask_cpf('{cpf}') with NULL-cpf grant returned {result!r}, "
            f"expected {MASKED_GLYPH!r}"
        )


# ---------- end-to-end on gold.dim_consultora ----------


def test_dim_consultora_returns_zero_rows_with_no_grant(spark, current_user):
    """AC: With acl_consultora containing only an admin row, SELECT * FROM
    gold.dim_consultora as current_user() returns zero rows. The row
    filter is the ON-clause."""
    _insert_acl(spark, SEED_ADMIN_EMAIL, "Sudeste", None)
    n = spark.sql(f"SELECT count(*) AS n FROM {DIM}").collect()[0]["n"]
    assert n == 0, (
        f"dim_consultora returned {n} rows for {current_user} with no grant — "
        "either RLS is not attached or owner-bypass is in effect (see module docstring)"
    )


def test_dim_consultora_filters_to_granted_region_with_cpf_masked(spark, current_user):
    """AC: After INSERT (current_user(), 'Sudeste', NULL), SELECT returns
    only Sudeste rows and cpf is masked across all returned rows."""
    _insert_acl(spark, current_user, "Sudeste", None)
    rows = spark.sql(
        f"SELECT regiao, cpf FROM {DIM} TABLESAMPLE (500 ROWS)"
    ).collect()
    assert rows, "dim_consultora returned no rows after granting Sudeste"
    for r in rows:
        assert r["regiao"] == "Sudeste", (
            f"non-Sudeste row leaked: regiao={r['regiao']!r}"
        )
        assert r["cpf"] == MASKED_GLYPH, (
            f"cpf not masked under NULL-cpf grant: {r['cpf']!r}"
        )


def test_dim_consultora_unmasks_one_specific_cpf(spark, current_user):
    """AC: After also INSERT (current_user(), 'Sudeste', '<real cpf>'),
    that one row's cpf is unmasked; other returned rows stay masked.

    Picks a real cpf from dim_consultora dynamically; this avoids
    coupling the test to seed values."""
    # First grant region-only access, then pick a real cpf from the
    # filtered view (we cannot read cpf raw without the grant).
    _insert_acl(spark, current_user, "Sudeste", None)
    sample = spark.sql(
        f"""
        SELECT consultora_id, cpf
        FROM {DIM}
        WHERE __END_AT IS NULL
        LIMIT 1
        """
    ).collect()
    if not sample:
        pytest.skip("no dim_consultora rows for Sudeste — SDP pipeline not run yet")
    # cpf is masked here; we need the raw cpf, so look it up from silver
    # which is upstream of the mask attachment. This is a test-only
    # backdoor — a demo presenter would INSERT a known cpf instead.
    consultora_id = sample[0]["consultora_id"]
    raw_cpf_row = spark.sql(
        f"SELECT cpf FROM {CATALOG}.silver.consultoras WHERE consultora_id = {consultora_id}"
    ).collect()
    if not raw_cpf_row:
        pytest.skip(f"silver.consultoras has no row for consultora_id={consultora_id}")
    target_cpf = raw_cpf_row[0]["cpf"]

    _insert_acl(spark, current_user, "Sudeste", target_cpf)

    # Now: this consultora's row should show cpf=target_cpf unmasked;
    # other Sudeste rows should still be masked.
    unmasked = spark.sql(
        f"""
        SELECT cpf
        FROM {DIM}
        WHERE consultora_id = {consultora_id}
          AND __END_AT IS NULL
        """
    ).collect()
    assert unmasked, "target consultora disappeared from filtered view"
    assert unmasked[0]["cpf"] == target_cpf, (
        f"target cpf still masked after grant: got {unmasked[0]['cpf']!r}, "
        f"expected {target_cpf!r}"
    )

    others = spark.sql(
        f"""
        SELECT cpf
        FROM {DIM}
        WHERE consultora_id <> {consultora_id}
          AND __END_AT IS NULL
        LIMIT 50
        """
    ).collect()
    if others:
        masked_count = sum(1 for r in others if r["cpf"] == MASKED_GLYPH)
        assert masked_count == len(others), (
            f"expected all non-granted rows masked, got {masked_count}/{len(others)} masked"
        )
