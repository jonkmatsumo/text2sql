#!/bin/bash
# Canonical dev bring-down script for Text2SQL stack

echo "Stopping Text2SQL Dev Stack..."

docker compose \
  -f docker-compose.infra.yml \
  -f docker-compose.app.yml \
  -f observability/docker-compose.observability.yml \
  down

echo "Stack stopped."
