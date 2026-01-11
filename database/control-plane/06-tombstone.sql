-- Add tombstone columns for cache invalidation
-- Tombstoned entries are marked invalid without deletion (for audit trail)

-- Add tombstone flag
ALTER TABLE semantic_cache ADD COLUMN IF NOT EXISTS is_tombstoned BOOLEAN DEFAULT FALSE;

-- Add tombstone reason for debugging
ALTER TABLE semantic_cache ADD COLUMN IF NOT EXISTS tombstone_reason TEXT;

-- Add tombstoned timestamp
ALTER TABLE semantic_cache ADD COLUMN IF NOT EXISTS tombstoned_at TIMESTAMP;

-- Index for filtering tombstoned entries
CREATE INDEX IF NOT EXISTS idx_cache_not_tombstoned
ON semantic_cache (tenant_id, is_tombstoned)
WHERE is_tombstoned = FALSE;

-- Comments
COMMENT ON COLUMN semantic_cache.is_tombstoned IS 'True if entry was invalidated due to constraint mismatch.';
COMMENT ON COLUMN semantic_cache.tombstone_reason IS 'Reason for tombstoning (e.g., rating_mismatch).';
