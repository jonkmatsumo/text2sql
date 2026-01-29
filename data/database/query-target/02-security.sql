-- Security hardening for Text2SQL
-- This script revokes dangerous privileges and enforces read-only access for the agent.

-- 1. Ensure mcp_reader role exists
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'mcp_reader') THEN
      CREATE ROLE mcp_reader WITH LOGIN PASSWORD 'mcp_secure_pass';
   END IF;
END
$$;

-- 2. Revoke all from public schema to be safe
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO mcp_reader;

-- 3. Grant SELECT on all existing tables in public schema
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_reader;

-- 4. Ensure future tables also have SELECT-only for mcp_reader
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_reader;

-- 5. Explicitly deny mutative commands at the role level if possible
-- Postgres doesn't have a direct "DENY INSERT", but we just don't GRANT it.
-- We can set the role to read-only mode by default for any session.
ALTER ROLE mcp_reader SET default_transaction_read_only = on;

-- 6. Clean up text2sql_ro if it was too permissive
-- Actually, the investigation found text2sql_ro has ALL PRIVILEGES.
-- Let's fix it to be truly read-only as well, or migrate everything to mcp_reader.
REVOKE ALL PRIVILEGES ON DATABASE pagila FROM text2sql_ro;
GRANT CONNECT ON DATABASE pagila TO text2sql_ro;
GRANT USAGE ON SCHEMA public TO text2sql_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO text2sql_ro;
ALTER ROLE text2sql_ro SET default_transaction_read_only = on;
