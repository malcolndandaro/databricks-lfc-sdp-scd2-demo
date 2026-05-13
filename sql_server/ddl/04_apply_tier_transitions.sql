-- 04_apply_tier_transitions.sql
--
-- Idempotent. Applies deterministic backdated UPDATEs that produce visible
-- tier-history events in the Change Tracking log. Run after 03_seed_data.sql,
-- typically as the second task (phase_2_transitions) of the sqlserver_setup
-- Job.
--
-- Hero Consultora 42 transition:
--   bronze -> prata, backdated to HERO_TRANSITION_DATE = 2026-02-01.
--   Must match src/seed/constants.py HERO_TRANSITION_DATE — Slice 03 places
--   12 Pedidos for her before this date and 12 after, and Slice 07's MV2
--   uses this as the SCD2 boundary for as-of comissão attribution.
--
-- Plus 50 additional Consultoras with deterministic transitions to give the
-- bronze.consultoras_raw -> dim_consultora SCD2 stream visible variety.
-- Selection rule: every Consultora whose id mod 10 == 0 (so ids 10, 20, …,
-- 500), excluding the hero. That gives 50 transitions deterministically.
--
-- Each transition sets the new tier and updates updated_at to a backdated
-- value so the Change Tracking event can be associated with a known point
-- in time. After this script, the demo's "live ouro promotion" beat is
-- still ahead — that runs interactively against this same table during
-- the customer demo.
--
-- Idempotency: each UPDATE only fires if the row's current tier differs
-- from the target tier. Re-running this script after a successful run is
-- a no-op.
--
-- {{schema}} is substituted at runtime by the notebook (crm_dev or crm_prod).

USE DemoDB;
GO

-- Hero Consultora #42 — bronze -> prata, 2026-02-01.
UPDATE {{schema}}.consultoras
SET tier = 'prata',
    updated_at = '2026-02-01 00:00:00.000'
WHERE consultora_id = 42
  AND tier <> 'prata';

PRINT CONCAT('Hero transition rows affected: ', @@ROWCOUNT);

-- Other 50 Consultoras — deterministic promotions one rung up.
-- (semente -> bronze, bronze -> prata, prata -> ouro, ouro -> diamante;
-- diamante stays at diamante since there's nowhere higher.)
-- Backdated to 2025-11-01 so all transitions predate today's `2026-05-08`
-- and can be sequenced against any Pedido in the 24-month window.
--
-- Idempotency guard (Slice 12 cleanup of Slice 04 drift): the
-- `updated_at < '2025-11-01'` filter ensures re-runs see zero candidate
-- rows after the first successful run. The 50 candidate Consultoras
-- (id mod 10 == 0, excluding hero #42 and the malformed-CPF id=7) are
-- forced to data_cadastro = 2025-09-01 in src/seed/generate_consultoras_sql.py
-- so they all qualify on the first run; after this UPDATE fires their
-- updated_at flips to 2025-11-01 and the guard makes subsequent runs a
-- no-op. Without this, re-running the script promotes the same 50 rows
-- one rung up each time.
UPDATE c
SET tier = CASE c.tier
              WHEN 'semente' THEN 'bronze'
              WHEN 'bronze'  THEN 'prata'
              WHEN 'prata'   THEN 'ouro'
              WHEN 'ouro'    THEN 'diamante'
              WHEN 'diamante' THEN 'diamante'
           END,
    updated_at = '2025-11-01 00:00:00.000'
FROM {{schema}}.consultoras c
WHERE c.consultora_id <> 42
  AND c.consultora_id % 10 = 0
  AND c.tier IN ('semente', 'bronze', 'prata', 'ouro')
  AND c.updated_at < '2025-11-01';

PRINT CONCAT('Other Consultora transition rows affected: ', @@ROWCOUNT);

GO
