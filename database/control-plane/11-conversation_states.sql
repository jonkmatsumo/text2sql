-- Conversation States table for multi-turn history
CREATE TABLE IF NOT EXISTS conversation_states (
    conversation_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL, -- For RLS and ownership
    state_version INTEGER NOT NULL,
    state_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL -- For TTL cleanup
);

-- Index for cleanup
CREATE INDEX IF NOT EXISTS idx_conversation_expires ON conversation_states(expires_at);
CREATE INDEX IF NOT EXISTS idx_conversation_user ON conversation_states(user_id);

-- Validates that state_json has required fields (min check)
ALTER TABLE conversation_states ADD CONSTRAINT check_state_structure
    CHECK (state_json ? 'conversation_id' AND state_json ? 'turns');

-- RLS
ALTER TABLE conversation_states ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view own conversations"
  ON conversation_states FOR SELECT
  USING (auth.uid()::text = user_id);

CREATE POLICY "Users can insert/update own conversations"
  ON conversation_states FOR ALL
  USING (auth.uid()::text = user_id);
