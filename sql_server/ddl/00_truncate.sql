-- 00_truncate.sql
--
-- Wipes {{schema}}.consultoras to its empty state so phase_1_seed re-inserts all
-- 500 rows (creating fresh Change Tracking INSERT events) and
-- phase_2_transitions actually fires UPDATEs (creating fresh CT UPDATE
-- events for hero #42's bronze->prata transition + 50 others).
--
-- Without this TRUNCATE, sqlserver_setup is mostly a no-op on subsequent
-- runs (phase_1_seed uses IF NOT EXISTS; phase_2_transitions has WHERE
-- guards that match already-transitioned rows). The two LFC syncs then
-- emit no events, and Slice 06's AutoCDC SCD2 in gold.dim_consultora
-- collapses to one row per consultora instead of the canonical
-- closed-bronze + open-prata pair for hero #42.
--
-- TRUNCATE on a CHANGE_TRACKING-enabled table also resets the per-table
-- CT version, which the LFC ingestion gateway uses to start the next
-- snapshot cleanly.
--
-- Idempotent — safe to run against an already-empty table.
--
-- {{schema}} is substituted at runtime by the notebook (crm_dev or crm_prod).

USE DemoDB;
GO

IF EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{{schema}}' AND t.name = 'consultoras'
)
BEGIN
    TRUNCATE TABLE {{schema}}.consultoras;
    PRINT 'TRUNCATE {{schema}}.consultoras complete.';
END
ELSE
BEGIN
    PRINT '{{schema}}.consultoras does not exist yet — phase_1_seed will create it.';
END;

GO
