#!/bin/bash
# Canonical dev bring-up script for Text2SQL stack (Infra + App + Observability)

set -e

echo "Starting Text2SQL Dev Stack (Infra + App + Observability)..."

docker compose \
  -f docker-compose.infra.yml \
  -f docker-compose.app.yml \
  -f docker-compose.observability.yml \
  up -d

echo "---------------------------------------------------"
echo "Stack is running!"
echo "Web UI:       http://localhost:8501"
echo "OTEL Worker:  http://localhost:4320"
echo "MLflow Sink:  http://localhost:5001"
echo "MinIO:        http://localhost:9001"
echo "---------------------------------------------------"
