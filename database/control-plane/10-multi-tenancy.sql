-- Control Plane Multi-Tenancy (Defense-in-Depth)
-- Enforce tenant isolation on metadata, cache, and evaluation tables.

-- 1. Helper function for current tenant
CREATE OR REPLACE FUNCTION current_tenant_id() RETURNS integer AS $$
DECLARE
    tenant_id_text text;
BEGIN
    tenant_id_text := current_setting('app.current_tenant', true);
    IF tenant_id_text IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN tenant_id_text::integer;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 2. Enable RLS on Control Plane tables
ALTER TABLE semantic_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE sql_examples ENABLE ROW LEVEL SECURITY;
ALTER TABLE golden_dataset ENABLE ROW LEVEL SECURITY;
-- Note: 'tenants' table may need global access or specific policy

-- 3. Policies

-- Semantic Cache Policy
CREATE POLICY isolation_semantic_cache ON semantic_cache
    FOR ALL
    USING (tenant_id = current_tenant_id());

-- SQL Examples (Few-Shot) Policy
-- Often shared, but if tenant-specific:
-- CREATE POLICY isolation_sql_examples ON sql_examples
--     FOR ALL
--     USING (tenant_id = current_tenant_id() OR tenant_id IS NULL);
-- Check implementation: currently few-shot might be global or tenant scoped. Assuming global for now?
-- Re-reading Phase 3 plan: "Retain RLS... few_shot_examples"
-- Let's stick to tenant_id match for now.
CREATE POLICY isolation_sql_examples ON sql_examples
    FOR ALL
    USING (tenant_id = current_tenant_id());

-- Golden Dataset Policy
CREATE POLICY isolation_golden_dataset ON golden_dataset
    FOR ALL
    USING (tenant_id = current_tenant_id());
