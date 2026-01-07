#!/bin/bash
set -e

# Create MLflow database for tracking server
# This must run before SQL scripts since we need to connect to postgres database
# MLflow will automatically create its schema tables on first connection

echo "Creating MLflow database..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE mlflow;
    GRANT ALL PRIVILEGES ON DATABASE mlflow TO $POSTGRES_USER;
EOSQL

echo "✓ MLflow database created"
