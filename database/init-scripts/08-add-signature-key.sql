-- Add signature_key column for exact-match cache lookup
-- This enables intent-based cache keying where PG != G != R

-- Add signature_key column (SHA256 hash of canonical signature JSON)
ALTER TABLE semantic_cache ADD COLUMN IF NOT EXISTS signature_key VARCHAR(64);

-- Add intent_signature column (full JSON for debugging/auditing)
ALTER TABLE semantic_cache ADD COLUMN IF NOT EXISTS intent_signature JSONB;

-- Index for exact signature lookup (primary path for cache hits)
CREATE INDEX IF NOT EXISTS idx_cache_signature_key
ON semantic_cache (tenant_id, signature_key, cache_type)
WHERE signature_key IS NOT NULL;

-- Comment for documentation
COMMENT ON COLUMN semantic_cache.signature_key IS 'SHA256 hash of canonical intent signature JSON. Primary lookup key.';
COMMENT ON COLUMN semantic_cache.intent_signature IS 'Full intent signature JSON with filters, ranking, entity, metric.';
