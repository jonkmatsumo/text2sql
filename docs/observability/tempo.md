# Tempo Trace Visualization

We have added [Grafana Tempo](https://grafana.com/oss/tempo/) to the observability stack to provide native, hierarchical trace visualization (waterfall views), alongside the existing Postgres-backed trace analytics.

## Architecture

- **OTEL Collector**: Dual-exports traces.
  - `otlp/tempo`: Sends traces to Tempo (port 4317).
  - `otlphttp/worker`: Sends traces to `otel-worker` (for MLflow/Postgres).
- **Tempo**: Runs in monolithic mode with local filesystem storage (`/tmp/tempo`).
  - **Ephemeral**: Data is lost if the container is recreated/volume cleared.
- **Grafana**: Provisioned with a `Tempo` datasource connecting to `http://tempo:3200`.

## Quick Start

1. Start the stack:
   ```bash
   docker compose -f docker-compose.infra.yml -f observability/docker-compose.observability.yml -f observability/docker-compose.grafana.yml up -d
   ```

2. Generate a test trace and verify end-to-end:
   ```bash
   ./scripts/observability/smoke_traces.sh
   # Script verifies:
   # 1. Trace generation (via verify_otel_setup.py)
   # 2. Postgres persistence (checks DB)
   # 3. Tempo ingestion (checks API)
   ```

3. View Traces:
   - Open Grafana: http://localhost:3000
   - Go to **Dashboards > Trace Explorer**
   - Click the link to open **Explore** with Tempo.
   - In "Search" tab, find your trace (e.g. by time or Trace ID from smoke test).
   - Click a Trace ID to see the waterfall.

## Troubleshooting

### "Permission Denied" in Tempo Logs
Tempo requires write access to `/tmp/tempo`. We run the container as `user: root` in `docker-compose.observability.yml` to ensure it can write to the volume.

### Missing Traces in Postgres
Run the persistence check script for diagnostics:
```bash
python3 scripts/observability/verify_postgres_persistence.py
```
If this fails:
- Check `otel-worker` logs.
- Check `otel.ingestion_queue` table in `agent_control` DB (script will warn if queue is backed up).
- Ensure `otel-collector` is running and properly connected to `otel-worker`.

### Missing Traces in Tempo
- Ensure `otel-collector` logs don't show connection errors to `tempo:4317`.
- Ensure Tempo is healthy: `docker ps | grep tempo`.
- Check if `smoke_traces.sh` reports success for Tempo API check.

## Operational Guardrails

> [!WARNING]
> **Tempo data is EPHEMERAL.**
> We use local filesystem storage for Tempo. If you remove the container volume or restart intimately, **traces will be lost**.
> Use Postgres/MLflow for durable analytics and history. Tempo is purely for real-time debugging and visualization.
