-- Pinned Recommendations Table
-- Tenant-scoped rules to always include specific examples for matching queries.

CREATE TABLE IF NOT EXISTS pinned_recommendations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id INT NOT NULL,
    match_type VARCHAR(20) NOT NULL CHECK (match_type IN ('exact', 'contains')),
    match_value TEXT NOT NULL,
    registry_example_ids JSONB NOT NULL DEFAULT '[]'::jsonb, -- List of canonical Example UUIDs (from query_pairs)
    priority INT NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for fast lookup by tenant (always filtered by tenant)
CREATE INDEX IF NOT EXISTS idx_pinned_recos_tenant ON pinned_recommendations(tenant_id);

-- Index for finding enabled rules quickly
CREATE INDEX IF NOT EXISTS idx_pinned_recos_enabled ON pinned_recommendations(enabled);

-- Trigger for updated_at
CREATE TRIGGER update_pinned_recos_modtime
    BEFORE UPDATE ON pinned_recommendations
    FOR EACH ROW
    EXECUTE PROCEDURE update_modified_column();
