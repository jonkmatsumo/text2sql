-- 1. Create the read-only user for the Agent
-- 'NOINHERIT' ensures it doesn't accidentally pick up superuser roles if grouped later.
CREATE USER bi_agent_ro WITH PASSWORD 'secure_agent_pass' NOINHERIT;

-- 2. Grant connection
GRANT CONNECT ON DATABASE pagila TO bi_agent_ro;

-- 3. Connect to the database context
\c pagila

-- 4. Grant Usage on Schema (allows "seeing" the schema exists)
GRANT USAGE ON SCHEMA public TO bi_agent_ro;

-- 5. Grant Select on ALL existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bi_agent_ro;

-- 6. Ensure future tables are readable (Critical for dynamic environments)
-- This fixes the common issue where new tables created by 'postgres' are invisible to the agent.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bi_agent_ro;

