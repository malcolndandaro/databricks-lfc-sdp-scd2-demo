-- 01_create_database.sql
--
-- Idempotent. Ensures the DemoDB database exists and has Change Tracking
-- enabled at database level. Run by the run_sqlserver_setup notebook
-- (Slice 02 Job, phase_1_seed task) before any table-level DDL.
--
-- Deviation from PRD: Change Tracking is used instead of full CDC. The
-- deployed instance ships with Change Tracking; Lakeflow Connect supports
-- both modes. See sql_server/INSTANCE.md for the rationale.
--
-- Re-runnable: every statement guards on existence.

USE master;

IF DB_ID('DemoDB') IS NULL
BEGIN
    PRINT 'Creating database DemoDB...';
    CREATE DATABASE DemoDB;
END
ELSE
BEGIN
    PRINT 'Database DemoDB already exists — skipping CREATE.';
END;

GO

USE DemoDB;

-- Enable Change Tracking at database level (idempotent)
IF NOT EXISTS (
    SELECT 1
    FROM sys.change_tracking_databases
    WHERE database_id = DB_ID('DemoDB')
)
BEGIN
    PRINT 'Enabling Change Tracking on DemoDB...';
    ALTER DATABASE DemoDB
    SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
END
ELSE
BEGIN
    PRINT 'Change Tracking already enabled on DemoDB — skipping.';
END;

GO
