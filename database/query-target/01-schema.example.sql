-- Example schema for query-target database
-- Replace with your actual business schema

CREATE TABLE IF NOT EXISTS example_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Add your business tables here
-- The actual schema is generated via download_data.sh
