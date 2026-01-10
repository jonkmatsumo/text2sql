
-- Add Unique Index to prevent duplicate cache entries
-- This supports ON CONFLICT DO NOTHING in the application layer
CREATE UNIQUE INDEX IF NOT EXISTS idx_semantic_cache_unique_query
ON semantic_cache (tenant_id, user_query, cache_type, schema_version);
