"""Shared constants for the Direct-Sales E2E demo's synthetic data.

These values are the source of truth for both this Python generator and the
T-SQL DDL in `sql_server/ddl/` (Slice 02). When changing a hero-related value,
update both — Slice 02's `04_apply_tier_transitions.sql` must backdate Consultora
#42's tier change to `HERO_TRANSITION_DATE` for the as-of MV (Slice 07) to
attribute pedidos to the correct tier on stage.
"""
from __future__ import annotations

from datetime import date

# --- Hero Consultora (the demo's SCD2 protagonist) ---
HERO_CONSULTORA_ID: int = 42
HERO_TIER_BEFORE: str = "bronze"
HERO_TIER_AFTER: str = "prata"
# Roughly 3 months before today's anchor; chosen so 24-month pedido windows
# straddle it cleanly. Slice 02's tier-transition DDL must use this date.
HERO_TRANSITION_DATE: date = date(2026, 2, 1)
# How many Pedidos hero #42 gets, split evenly across the transition.
HERO_PEDIDOS_TOTAL: int = 24

# --- Scale ---
N_CONSULTORAS: int = 500
N_PEDIDOS_DEFAULT: int = 50_000
PEDIDOS_PER_FILE: int = 1_000  # → ~50 parquet files at 50k Pedidos

# --- Bad-row plant counts (drives the silver Expectations beat) ---
BAD_NEGATIVE_VALOR_COUNT: int = 5
BAD_FUTURE_DATE_COUNT: int = 3
BAD_FUTURE_YEAR: int = 2099

# --- Distributions (per PRD) ---
TIER_DISTRIBUTION: dict[str, float] = {
    "semente": 0.40,
    "bronze": 0.30,
    "prata": 0.18,
    "ouro": 0.10,
    "diamante": 0.02,
}

STATUS_DISTRIBUTION: dict[str, float] = {
    "entregue": 0.70,
    "enviado": 0.15,
    "confirmado": 0.08,
    "cancelado": 0.05,
    "criado": 0.02,
}

FORMA_PAGAMENTO_DISTRIBUTION: dict[str, float] = {
    "pix": 0.50,
    "cartao": 0.30,
    "boleto": 0.20,
}

REGIONS: tuple[str, ...] = (
    "Sudeste",
    "Sul",
    "Nordeste",
    "Centro-Oeste",
    "Norte",
)

# Lognormal centred on R$ 250 with tail to ~R$ 5k.
# valor = exp(mu + sigma * Z). exp(5.5) ≈ 244.7, +2σ → ~R$ 1.2k, +3σ → ~R$ 5.5k.
VALOR_TOTAL_LOGNORMAL_MEAN: float = 5.5
VALOR_TOTAL_LOGNORMAL_SIGMA: float = 0.8

# Pedido data window: 24 months ending at `today` (caller-provided so tests are
# deterministic regardless of wall clock).
DATA_PEDIDO_WINDOW_MONTHS: int = 24
