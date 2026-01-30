-- Migration: 002_ingestion_templates
-- Description: Create ingestion_templates table for reusable ingestion configurations

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM _migrations WHERE name = '002_ingestion_templates') THEN
        RAISE NOTICE 'Migration 002_ingestion_templates already applied, skipping';
        RETURN;
    END IF;

    -- ============================================================================
    -- ingestion_templates table
    -- ============================================================================
    CREATE TABLE IF NOT EXISTS ingestion_templates (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        config JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Trigger for updated_at
    CREATE TRIGGER update_ingestion_templates_modtime
        BEFORE UPDATE ON ingestion_templates
        FOR EACH ROW
        EXECUTE PROCEDURE update_modified_column();

    RAISE NOTICE 'Created ingestion_templates table';

    -- Record migration
    INSERT INTO _migrations (name) VALUES ('002_ingestion_templates');
    RAISE NOTICE 'Migration 002_ingestion_templates applied successfully';

END $$;
