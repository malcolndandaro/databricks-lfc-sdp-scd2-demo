-- 02_create_tables.sql
--
-- Idempotent. Creates the {{schema}} schema and {{schema}}.consultoras table with the
-- PRD column set, then enables Change Tracking on the table so Lakeflow
-- Connect (Slice 04) can stream change events into bronze.consultoras_raw.
--
-- Schema reference (domain glossary):
--   Consultora — direct-sales rep. Tier ranking determines comissão.
--   Tiers (ascending): semente -> bronze -> prata -> ouro -> diamante.
--
-- Pedidos do NOT exist in SQL Server. They live only in the UC Volume per
-- the source-split decision in the PRD; Auto Loader ingests them in Slice 03.
--
-- {{schema}} is substituted at runtime by the notebook (crm_dev or crm_prod).

USE DemoDB;
GO

-- Idempotent CREATE SCHEMA
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{{schema}}')
BEGIN
    PRINT 'Creating schema {{schema}}...';
    EXEC('CREATE SCHEMA {{schema}}');
END
ELSE
BEGIN
    PRINT 'Schema {{schema}} already exists — skipping.';
END;

GO

-- Idempotent CREATE TABLE {{schema}}.consultoras
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '{{schema}}' AND t.name = 'consultoras'
)
BEGIN
    PRINT 'Creating {{schema}}.consultoras...';
    CREATE TABLE {{schema}}.consultoras (
        consultora_id INT NOT NULL,
        cpf VARCHAR(11) NOT NULL,
        nome NVARCHAR(200) NOT NULL,
        email VARCHAR(255) NULL,
        regiao VARCHAR(50) NOT NULL,
        tier VARCHAR(20) NOT NULL,
        data_cadastro DATETIME2(3) NOT NULL CONSTRAINT DF_consultoras_data_cadastro DEFAULT SYSUTCDATETIME(),
        ativo BIT NOT NULL CONSTRAINT DF_consultoras_ativo DEFAULT 1,
        updated_at DATETIME2(3) NOT NULL CONSTRAINT DF_consultoras_updated_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_consultoras PRIMARY KEY CLUSTERED (consultora_id)
    );
END
ELSE
BEGIN
    PRINT 'Table {{schema}}.consultoras already exists — skipping CREATE.';
END;

GO

-- Indexes (idempotent)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_consultoras_tier_regiao' AND object_id = OBJECT_ID('{{schema}}.consultoras'))
BEGIN
    PRINT 'Creating IX_consultoras_tier_regiao...';
    CREATE INDEX IX_consultoras_tier_regiao ON {{schema}}.consultoras (tier, regiao);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_consultoras_updated_at' AND object_id = OBJECT_ID('{{schema}}.consultoras'))
BEGIN
    PRINT 'Creating IX_consultoras_updated_at...';
    CREATE INDEX IX_consultoras_updated_at ON {{schema}}.consultoras (updated_at);
END;

GO

-- Enable Change Tracking on {{schema}}.consultoras (idempotent)
IF NOT EXISTS (
    SELECT 1 FROM sys.change_tracking_tables
    WHERE object_id = OBJECT_ID('{{schema}}.consultoras')
)
BEGIN
    PRINT 'Enabling Change Tracking on {{schema}}.consultoras...';
    ALTER TABLE {{schema}}.consultoras
    ENABLE CHANGE_TRACKING
    WITH (TRACK_COLUMNS_UPDATED = ON);
END
ELSE
BEGIN
    PRINT 'Change Tracking already enabled on {{schema}}.consultoras — skipping.';
END;

GO
