-- Migration: 005_query_target_configs
-- Description: Add query_target_configs table for UI-driven query-target settings
--
-- Usage:
--   psql -h $CONTROL_DB_HOST -U $CONTROL_DB_USER -d $CONTROL_DB_NAME -f scripts/migrations/005_query_target_configs.sql
--   Or via docker: docker compose exec control-db psql -U postgres -d agent_control -f /migrations/005_query_target_configs.sql

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM _migrations WHERE name = '005_query_target_configs') THEN
        RAISE NOTICE 'Migration 005_query_target_configs already applied, skipping';
        RETURN;
    END IF;

    CREATE TABLE IF NOT EXISTS query_target_configs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        provider TEXT NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        auth JSONB NOT NULL DEFAULT '{}'::jsonb,
        guardrails JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL
            CHECK (status IN ('inactive', 'pending', 'active', 'unhealthy')),
        last_tested_at TIMESTAMPTZ,
        last_test_status TEXT,
        last_error_code TEXT,
        last_error_message TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        activated_at TIMESTAMPTZ,
        deactivated_at TIMESTAMPTZ
    );

    CREATE INDEX IF NOT EXISTS idx_query_target_configs_provider
        ON query_target_configs(provider);

    CREATE UNIQUE INDEX IF NOT EXISTS uq_query_target_configs_active
        ON query_target_configs(status)
        WHERE status = 'active';

    CREATE UNIQUE INDEX IF NOT EXISTS uq_query_target_configs_pending
        ON query_target_configs(status)
        WHERE status = 'pending';

    CREATE OR REPLACE FUNCTION update_query_target_configs_modtime()
    RETURNS TRIGGER AS $func$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $func$ language 'plpgsql';

    DROP TRIGGER IF EXISTS update_query_target_configs_modtime ON query_target_configs;
    CREATE TRIGGER update_query_target_configs_modtime
        BEFORE UPDATE ON query_target_configs
        FOR EACH ROW
        EXECUTE PROCEDURE update_query_target_configs_modtime();

    INSERT INTO _migrations (name) VALUES ('005_query_target_configs');
    RAISE NOTICE 'Migration 005_query_target_configs applied successfully';

END $$;
