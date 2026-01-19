-- 10-evaluation.sql
-- Control-Plane tables for Evaluation Runs and Results

CREATE TABLE IF NOT EXISTS evaluation_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_mode TEXT NOT NULL,
    dataset_version TEXT,
    git_sha TEXT,
    tenant_id INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    config_snapshot JSONB DEFAULT '{}'::jsonb,
    metrics_summary JSONB,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_eval_runs_tenant ON evaluation_runs(tenant_id);
CREATE INDEX IF NOT EXISTS idx_eval_runs_dataset_started ON evaluation_runs(dataset_mode, started_at DESC);

CREATE TABLE IF NOT EXISTS evaluation_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES evaluation_runs(id) ON DELETE CASCADE,
    test_id TEXT NOT NULL,
    question TEXT NOT NULL,
    generated_sql TEXT,
    is_correct BOOLEAN NOT NULL,
    structural_score FLOAT,
    error_message TEXT,
    execution_time_ms INTEGER,
    raw_response JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_eval_results_run_id ON evaluation_results(run_id);
CREATE INDEX IF NOT EXISTS idx_eval_results_test_id ON evaluation_results(test_id);

-- Enable Row Level Security
ALTER TABLE evaluation_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE evaluation_results ENABLE ROW LEVEL SECURITY;

-- Policies
-- evaluation_runs:
--   - Select/Insert/Update for current tenant
CREATE POLICY eval_runs_tenant_isolation ON evaluation_runs
    USING (tenant_id = current_setting('app.current_tenant', true)::integer);

-- evaluation_results:
--   - RLS inherited via join or just open if run is accessible?
--   - Standard pattern: Add tenant_id to child table OR join policy.
--   - Let's add tenant_id to evaluation_results to make RLS cleaner and performant.

ALTER TABLE evaluation_results ADD COLUMN IF NOT EXISTS tenant_id INTEGER NOT NULL DEFAULT 1;
CREATE INDEX IF NOT EXISTS idx_eval_results_tenant ON evaluation_results(tenant_id);

CREATE POLICY eval_results_tenant_isolation ON evaluation_results
    USING (tenant_id = current_setting('app.current_tenant', true)::integer);
