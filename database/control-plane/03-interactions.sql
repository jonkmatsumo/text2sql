-- Interactions source of truth for feedback loops
CREATE TABLE query_interactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id TEXT, -- Can be null or link to external conversation system
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_snapshot_id TEXT NOT NULL,
    user_nlq_text TEXT NOT NULL,
    generated_sql TEXT,
    response_payload JSONB, -- Storing full response (text + metadata)
    execution_status TEXT CHECK (execution_status IN ('SUCCESS', 'FAILURE')),
    error_type TEXT,
    model_version TEXT,
    prompt_version TEXT,
    tables_used TEXT[], -- Array of strings
    trace_id TEXT,      -- Link to MLflow/LangSmith
    tenant_id INTEGER   -- Multi-tenancy support
);

-- Indexes for common lookups
CREATE INDEX idx_interactions_conversation ON query_interactions(conversation_id);
CREATE INDEX idx_interactions_created ON query_interactions(created_at DESC);
CREATE INDEX idx_interactions_status ON query_interactions(execution_status);

-- Enable RLS (though usually open for this system)
ALTER TABLE query_interactions ENABLE ROW LEVEL SECURITY;

-- Allow service role full access (default)
CREATE POLICY service_role_full_access ON query_interactions
    USING (true)
    WITH CHECK (true);
