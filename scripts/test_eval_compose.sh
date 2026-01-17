#!/bin/bash
set -e

echo "Running smoke tests for Airflow Evaluation Compose..."

# Check if docker compose file exists
if [ ! -f "docker-compose.evals.yml" ]; then
    echo "ERROR: docker-compose.evals.yml not found!"
    exit 1
fi

# detailed check
if ! grep -q "airflow-webserver" docker-compose.evals.yml; then
    echo "ERROR: airflow-webserver service missing in compose file"
    exit 1
fi

if ! grep -q "airflow-scheduler" docker-compose.evals.yml; then
    echo "ERROR: airflow-scheduler service missing in compose file"
    exit 1
fi

# Validate compose config (if dry run is possible without pulling images, otherwise just check syntax)
# This command validates the configuration without starting services
if docker compose -f docker-compose.evals.yml config > /dev/null; then
    echo "SUCCESS: docker compose config is valid."
else
    echo "ERROR: docker compose config failed validation."
    exit 1
fi

echo "Smoke tests passed!"
