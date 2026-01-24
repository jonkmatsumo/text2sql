#!/bin/bash
set -e

# Resolve repo root
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# Base directory for local data
DATA_DIR="./local-data"

# Subdirectories for each service
SERVICES=(
    "postgres-db"
    "agent-control-db"
    "minio"
    "memgraph"
    "airflow"
)

echo "Bootstrapping local-data directories in ${DATA_DIR}..."

if [ ! -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR"
    echo "Created base directory: ${DATA_DIR}"
fi

for service in "${SERVICES[@]}"; do
    TARGET_PATH="${DATA_DIR}/${service}"
    if [ ! -d "$TARGET_PATH" ]; then
        mkdir -p "$TARGET_PATH"
        echo "Created directory: ${TARGET_PATH}"
        # Set permissions to ensure Docker containers (running as various users) can write.
        # This is a broad permission setting safe for local dev environments to avoid
        # complex user mapping issues with Postgres/MinIO images.
        chmod 777 "$TARGET_PATH"
    else
        echo "Directory already exists: ${TARGET_PATH}"
    fi
done

echo "Bootstrap complete. State directories are ready for Docker bind mounts."
