-- Migration: 004_synth_generation_runs
-- Description: Create synth_generation_runs and synth_templates tables for persisted synth executions

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM _migrations WHERE name = '004_synth_generation_runs') THEN
        RAISE NOTICE 'Migration 004_synth_generation_runs already applied, skipping';
        RETURN;
    END IF;

    -- ============================================================================
    -- synth_templates table (optional stub)
    -- ============================================================================
    CREATE TABLE IF NOT EXISTS synth_templates (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        config JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Trigger for updated_at
    DROP TRIGGER IF EXISTS update_synth_templates_modtime ON synth_templates;
    CREATE TRIGGER update_synth_templates_modtime
        BEFORE UPDATE ON synth_templates
        FOR EACH ROW
        EXECUTE PROCEDURE update_modified_column();

    RAISE NOTICE 'Created synth_templates table';

    -- ============================================================================
    -- synth_generation_runs table
    -- ============================================================================
    CREATE TABLE IF NOT EXISTS synth_generation_runs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        status TEXT NOT NULL
            CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELED')),
        started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMPTZ,
        config_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
        ui_state JSONB DEFAULT '{}'::jsonb,
        output_path TEXT,
        manifest JSONB,
        metrics JSONB DEFAULT '{}'::jsonb,
        error_message TEXT,
        job_id UUID, -- References ops_jobs(id)
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_synth_runs_started_at ON synth_generation_runs(started_at DESC);
    CREATE INDEX IF NOT EXISTS idx_synth_runs_status ON synth_generation_runs(status);
    CREATE INDEX IF NOT EXISTS idx_synth_runs_job_id ON synth_generation_runs(job_id);

    RAISE NOTICE 'Created synth_generation_runs table';

    -- Record migration
    INSERT INTO _migrations (name) VALUES ('004_synth_generation_runs');
    RAISE NOTICE 'Migration 004_synth_generation_runs applied successfully';

END $$;
