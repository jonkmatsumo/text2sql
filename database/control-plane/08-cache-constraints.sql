-- Add Unique Index to prevent duplicate cache entries across tenants
-- This supports ON CONFLICT DO UPDATE/NOTHING for global cache hits
CREATE UNIQUE INDEX IF NOT EXISTS idx_semantic_cache_global
ON semantic_cache (signature_key, cache_type, schema_version);
