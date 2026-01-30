-- Migration: 001_control_plane_schema
-- Description: Create control-plane database tables (ops_jobs, pinned_recommendations)
-- Previously: These tables were created on application startup via ensure_*_schema methods
-- Now: Run this migration once before starting services
--
-- Usage:
--   psql -h $CONTROL_DB_HOST -U $CONTROL_DB_USER -d $CONTROL_DB_NAME -f scripts/migrations/001_control_plane_schema.sql
--   Or via docker: docker compose exec control-db psql -U postgres -d agent_control -f /migrations/001_control_plane_schema.sql

-- ============================================================================
-- Migrations tracking table (idempotent)
-- ============================================================================
CREATE TABLE IF NOT EXISTS _migrations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ DEFAULT NOW()
);

-- Skip if already applied
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM _migrations WHERE name = '001_control_plane_schema') THEN
        RAISE NOTICE 'Migration 001_control_plane_schema already applied, skipping';
        RETURN;
    END IF;

    -- ============================================================================
    -- ops_jobs table
    -- ============================================================================
    CREATE TABLE IF NOT EXISTS ops_jobs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        job_type TEXT NOT NULL,
        status TEXT NOT NULL
            CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
        started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMPTZ,
        error_message TEXT,
        payload JSONB DEFAULT '{}'::jsonb,
        result JSONB DEFAULT '{}'::jsonb
    );

    RAISE NOTICE 'Created ops_jobs table';

    -- ============================================================================
    -- pinned_recommendations table
    -- ============================================================================
    CREATE TABLE IF NOT EXISTS pinned_recommendations (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        tenant_id INT NOT NULL,
        match_type VARCHAR(20) NOT NULL CHECK (match_type IN ('exact', 'contains')),
        match_value TEXT NOT NULL,
        registry_example_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
        priority INT NOT NULL DEFAULT 0,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_pinned_recos_tenant
        ON pinned_recommendations(tenant_id);

    CREATE INDEX IF NOT EXISTS idx_pinned_recos_enabled
        ON pinned_recommendations(enabled);

    -- Trigger function for updated_at
    CREATE OR REPLACE FUNCTION update_modified_column()
    RETURNS TRIGGER AS $func$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $func$ language 'plpgsql';

    -- Trigger
    DROP TRIGGER IF EXISTS update_pinned_recos_modtime ON pinned_recommendations;
    CREATE TRIGGER update_pinned_recos_modtime
        BEFORE UPDATE ON pinned_recommendations
        FOR EACH ROW
        EXECUTE PROCEDURE update_modified_column();

    RAISE NOTICE 'Created pinned_recommendations table';

    -- Record migration
    INSERT INTO _migrations (name) VALUES ('001_control_plane_schema');
    RAISE NOTICE 'Migration 001_control_plane_schema applied successfully';

END $$;
