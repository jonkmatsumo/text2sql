-- Conversation States table for multi-turn history
CREATE TABLE IF NOT EXISTS conversation_states (
    conversation_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL, -- For RLS and ownership
    tenant_id INTEGER NOT NULL DEFAULT 1, -- Tenant scoping guard
    state_version INTEGER NOT NULL,
    state_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL -- For TTL cleanup
);

-- Backward-compatible migration for existing deployments
ALTER TABLE conversation_states
    ADD COLUMN IF NOT EXISTS tenant_id INTEGER NOT NULL DEFAULT 1;

-- Index for cleanup
CREATE INDEX IF NOT EXISTS idx_conversation_expires ON conversation_states(expires_at);
CREATE INDEX IF NOT EXISTS idx_conversation_user ON conversation_states(user_id);
CREATE INDEX IF NOT EXISTS idx_conversation_tenant ON conversation_states(tenant_id);

-- Validates that state_json has required fields (min check)
ALTER TABLE conversation_states ADD CONSTRAINT check_state_structure
    CHECK (state_json ? 'conversation_id' AND state_json ? 'turns');

-- RLS
ALTER TABLE conversation_states ENABLE ROW LEVEL SECURITY;

-- Allow full access for now (managed by app layer)
CREATE POLICY service_role_full_access ON conversation_states
    USING (true)
    WITH CHECK (true);
