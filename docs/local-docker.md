# Local Docker Persistence

As of **Phase 3** of the Docker Optimization effort, persistent infrastructure data (Postgres, MinIO, Memgraph) is no longer stored in hidden Docker volumes.

Data is now persisted to the host filesystem at:

```
./local-data/
  ├── postgres-db/      # query-target database (synthetic or pagila)
  ├── agent-control-db/ # agent control plane data
  ├── minio/            # MLflow artifacts
  └── memgraph/         # graph db data
```

This directory is `.gitignore`d, so your local data remains private and is not committed.

## Why this change?

1. **Visibility**: You can easily inspect, backup, or delete data files.
2. **Stability**: Docker VM disk usage is reduced; large data lives on your Mac filesystem.
3. **Reset**: To completely reset your environment, you can simply `rm -rf local-data/*` and restart (which will re-initialize schemas).

## Setup

Before running `docker compose up`, run the bootstrap script to create the directories with correct permissions:

```bash
./scripts/bootstrap_local_data.sh
```

## Running the Environment

### 1. Infrastructure (Pull-and-Run)

Start infrastructure services. This **does not require building** and uses pinned images.

```bash
docker compose -f docker-compose.infra.yml up -d
```

### 2. Application (Build)

Start application services (including API server, Streamlit app, and workers). This **rebuilds** local code changes.

```bash
docker compose -f docker-compose.infra.yml -f docker-compose.app.yml up -d --build
```

### 3. Observability (Optional)

To enable the observability stack (OTEL Collector):

```bash
docker compose -f docker-compose.infra.yml -f observability/docker-compose.observability.yml up -d
```

## Development Workflow (Hot Reload)

To support rapid iteration, source code is bind-mounted into containers. This means changes to files on your host machine are immediately reflected in the container (hot reload).

**Mounted Directories:**
- `streamlit/` -> Streamlit App
- `agent/` -> Agent Logic
- `mcp-server/src` -> MCP Server Logic
- `observability/otel-worker/src` -> OTEL Worker

**Note:** Large directories like `local-data/`, `.git/`, and `docs/` are **not** mounted to improve performance. Only specific source and configuration files are shared.

Repeate for `text2sql_control_pg_data`, `text2sql_mlflow_artifacts`, etc.

## Troubleshooting

### Persistence Errors (Permission Denied)
If you see "Permission denied" errors in Docker logs:

1. Ensure you ran `./scripts/bootstrap_local_data.sh`.
2. Check permissions: `ls -la local-data/`.
3. If necessary, allow generic write access (safe for local dev):
   ```bash
   chmod -R 777 local-data/
   ```
