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
ALTER TABLE query_pairs ENABLE ROW LEVEL SECURITY;

-- 3. Policies

-- Unified Registry Policy
CREATE POLICY isolation_query_pairs ON query_pairs
    FOR ALL
    USING (tenant_id = current_tenant_id());
