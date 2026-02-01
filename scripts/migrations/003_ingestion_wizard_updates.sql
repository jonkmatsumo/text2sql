-- Migration: 003_ingestion_wizard_updates
-- Description: Add ROLLED_BACK status, deleted_at column, and ingestion_templates table if not exists

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM _migrations WHERE name = '003_ingestion_wizard_updates') THEN
        RAISE NOTICE 'Migration 003_ingestion_wizard_updates already applied, skipping';
        RETURN;
    END IF;

    -- Update nlp_pattern_runs status constraint
    -- Note: We assume the constraint name or use a more generic approach
    -- Find constraint name
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'nlp_pattern_runs') THEN
        DECLARE
            con_name text;
        BEGIN
            SELECT conname INTO con_name
            FROM pg_constraint
            WHERE conrelid = 'nlp_pattern_runs'::regclass
            AND contype = 'c'
            AND pg_get_constraintdef(oid) LIKE '%status%';

            if con_name IS NOT NULL THEN
                EXECUTE 'ALTER TABLE nlp_pattern_runs DROP CONSTRAINT ' || con_name;
            END IF;
        END;

        ALTER TABLE nlp_pattern_runs
        ADD CONSTRAINT nlp_pattern_runs_status_check
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'AWAITING_REVIEW', 'ROLLED_BACK'));
    END IF;

    -- Add deleted_at to nlp_patterns
    IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'nlp_patterns') THEN
        ALTER TABLE nlp_patterns ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP;
    END IF;

    -- Record migration
    INSERT INTO _migrations (name) VALUES ('003_ingestion_wizard_updates');
    RAISE NOTICE 'Migration 003_ingestion_wizard_updates applied successfully';

END $$;
