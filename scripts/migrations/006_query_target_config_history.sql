-- Migration: 006_query_target_config_history
-- Description: Add history/audit table for query-target configuration events
--
-- Usage:
--   psql -h $CONTROL_DB_HOST -U $CONTROL_DB_USER -d $CONTROL_DB_NAME -f scripts/migrations/006_query_target_config_history.sql
--   Or via docker: docker compose exec control-db psql -U postgres -d agent_control -f /migrations/006_query_target_config_history.sql

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM _migrations WHERE name = '006_query_target_config_history') THEN
        RAISE NOTICE 'Migration 006_query_target_config_history already applied, skipping';
        RETURN;
    END IF;

    CREATE TABLE IF NOT EXISTS query_target_config_history (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        config_id UUID NOT NULL REFERENCES query_target_configs(id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_query_target_config_history_config_id
        ON query_target_config_history(config_id);

    CREATE INDEX IF NOT EXISTS idx_query_target_config_history_created_at
        ON query_target_config_history(created_at DESC);

    INSERT INTO _migrations (name) VALUES ('006_query_target_config_history');
    RAISE NOTICE 'Migration 006_query_target_config_history applied successfully';

END $$;
