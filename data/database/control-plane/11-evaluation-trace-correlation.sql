-- 11-evaluation-trace-correlation.sql
-- Add trace_id to evaluation_results for correlation with OTEL traces

ALTER TABLE evaluation_results ADD COLUMN IF NOT EXISTS trace_id TEXT;
CREATE INDEX IF NOT EXISTS idx_eval_results_trace_id ON evaluation_results(trace_id);
