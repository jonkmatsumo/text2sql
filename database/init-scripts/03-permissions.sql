-- 1. Create the read-only user for the Agent
-- 'NOINHERIT' ensures it doesn't accidentally pick up superuser roles if grouped later.
CREATE USER text2sql_ro WITH PASSWORD 'secure_agent_pass' NOINHERIT;

-- 2. Grant connection
GRANT CONNECT ON DATABASE pagila TO text2sql_ro;

-- 3. Connect to the database context
\c pagila

-- 4. Grant Usage on Schema (allows "seeing" the schema exists)
GRANT USAGE ON SCHEMA public TO text2sql_ro;

-- 5. Grant Select on ALL existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO text2sql_ro;

-- 6. Ensure future tables are readable (Critical for dynamic environments)
-- This fixes the common issue where new tables created by 'postgres' are invisible to the agent.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO text2sql_ro;

-- 7. Setup Semantic Layer Mock (Phase 1 Requirement)
-- This table acts as a simple Metric Store for the MVP.
CREATE TABLE IF NOT EXISTS public.semantic_definitions (
    term_id SERIAL PRIMARY KEY,
    term_name TEXT NOT NULL,
    definition TEXT NOT NULL,
    sql_logic TEXT
);

INSERT INTO public.semantic_definitions (term_name, definition, sql_logic) VALUES
('High Value Customer', 'Customer with lifetime payments > $150', 'SUM(amount) > 150'),
('Churned', 'No rental activity in the last 30 days', 'last_rental_date < NOW() - INTERVAL ''30 days'''),
('Gross Revenue', 'Total sum of all payments', 'SUM(amount) FROM payment');

-- Grant access to the semantic layer
GRANT SELECT ON public.semantic_definitions TO text2sql_ro;
