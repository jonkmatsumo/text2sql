-- Add partial unique index on trace_id for idempotent interaction creation
-- Only applies when trace_id is NOT NULL (allows multiple NULL values)
CREATE UNIQUE INDEX IF NOT EXISTS idx_interactions_trace_id_unique
    ON query_interactions (trace_id)
    WHERE trace_id IS NOT NULL;
