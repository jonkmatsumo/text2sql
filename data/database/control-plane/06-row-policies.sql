-- Row-level Security Policies Configuration
-- This schema allows the agent to enforce "RLS-like" row filtering at the application layer
-- without requiring consumers to modify their database permissions/RLS policies.

CREATE TABLE IF NOT EXISTS row_policies (
    policy_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,           -- e.g. 'customer'
    tenant_column TEXT NOT NULL,        -- e.g. 'store_id'
    policy_expression TEXT NOT NULL,    -- e.g. '{column} = :tenant_id'
    is_enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Ensure only one active policy per table
    CONSTRAINT uq_active_table_policy UNIQUE (table_name)
);

-- Internal policies for the control-plane DB itself (defense-in-depth)
-- We enforce these on the agent connection, but we also document them here.
-- Note: 'tenants', 'api_keys', etc.

-- Seed policies for the Query Target schema
-- These mirror the logic previously enforced by database RLS in 05-multi-tenancy.sql
INSERT INTO row_policies (table_name, tenant_column, policy_expression)
VALUES
    ('customer', 'store_id', '{column} = :tenant_id'),
    ('rental', 'store_id', '{column} = :tenant_id'),
    ('payment', 'store_id', '{column} = :tenant_id'),
    ('staff', 'store_id', '{column} = :tenant_id'),
    ('inventory', 'store_id', '{column} = :tenant_id')
ON CONFLICT (table_name) DO UPDATE
SET
    tenant_column = EXCLUDED.tenant_column,
    policy_expression = EXCLUDED.policy_expression;
