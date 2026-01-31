#!/bin/bash
# Pre-flight check for port availability
# Prevents opaque Docker errors by failing fast if configured ports are taken.

set -e

# Load .env if present
if [ -f .env ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    # Skip comments and empty lines
    if [[ ! "$line" =~ ^# ]] && [[ "$line" =~ = ]]; then
      export "$line"
    fi
  done < .env
fi

# Resolve ports with defaults matching docker-compose files
MEMGRAPH_LAB_PORT=${MEMGRAPH_LAB_PORT:-3000}
MINIO_PORT=${MINIO_PORT:-9000}
MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT:-9001}
UI_DOCKER_PORT=${UI_PORT:-3333}

# Function to check a single port
check_port() {
  local port=$1
  local service=$2

  # Try nc (netcat) first, usually available
  if command -v nc >/dev/null 2>&1; then
    if nc -z 127.0.0.1 "$port" 2>/dev/null; then
      echo "❌ Port $port is in use ($service). Update .env or stop the conflicting process."
      return 1
    fi
  # Fallback to lsof
  elif command -v lsof >/dev/null 2>&1; then
    if lsof -i :"$port" -t >/dev/null 2>&1; then
      echo "❌ Port $port is in use ($service). Update .env or stop the conflicting process."
      return 1
    fi
  else
    echo "⚠️  Cannot check port $port (nc/lsof not found). Skipping."
  fi
  return 0
}

echo "🔍 Checking port availability..."
failed=0

check_port "$MEMGRAPH_LAB_PORT" "Memgraph Lab" || failed=1
check_port "$MINIO_PORT" "MinIO API" || failed=1
check_port "$MINIO_CONSOLE_PORT" "MinIO Console" || failed=1
check_port "$UI_DOCKER_PORT" "UI (Docker)" || failed=1

if [ "$failed" -eq 1 ]; then
  echo "Aborting startup due to port conflicts."
  exit 1
fi

echo "✅ Ports available."
exit 0
