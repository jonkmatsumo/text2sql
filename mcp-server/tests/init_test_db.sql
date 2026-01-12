-- Create read-only role if it doesn't exist
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'text2sql_ro') THEN

      CREATE ROLE text2sql_ro WITH LOGIN PASSWORD 'test_password';
   END IF;
END
$do$;

-- Create database if it doesn't exist (this is usually handled by POSTGRES_DB env var, but good for completeness in other contexts)
-- Note: CREATE DATABASE cannot run inside a transaction block, which is common in some init flows.
-- Skipped here as POSTGRES_DB=test_db is standard, but we need to ensure text2sql exists if that's what app uses.
-- However, inside a DO block or transaction we can't create DB.
-- The postgres docker image runs *scripts* in /docker-entrypoint-initdb.d/

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE test_db TO text2sql_ro;
ALTER DATABASE test_db OWNER TO text2sql_ro;
