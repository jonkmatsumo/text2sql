#!/bin/bash
# Trigger manual graph seeding
echo "Starting graph ingestion..."
docker compose exec seeder python -m mcp_server.seeding.cli
