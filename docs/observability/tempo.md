# Tempo Trace Visualization

We have added [Grafana Tempo](https://grafana.com/oss/tempo/) to the observability stack to provide native, hierarchical trace visualization (waterfall views), alongside the existing Postgres-backed trace analytics.

## Architecture

- **OTEL Collector**: Dual-exports traces.
  - `otlp/tempo`: Sends traces to Tempo (port 4317).
  - `otlphttp/worker`: Sends traces to `otel-worker` (for MLflow/Postgres).
- **Tempo**: Runs in monolithic mode with local filesystem storage (`/tmp/tempo`).
- **Grafana**: Provisioned with a `Tempo` datasource connecting to `http://tempo:3200`.

## Quick Start

1. Start the stack:
   ```bash
   docker compose -f docker-compose.infra.yml -f observability/docker-compose.observability.yml -f observability/docker-compose.grafana.yml up -d
   ```

2. Generate a test trace:
   ```bash
   ./scripts/observability/smoke_traces.sh
   # Or manually:
   # PYTHONPATH=agent/src:common/src:schema/src python3 scripts/verify_otel_setup.py
   ```

3. View Traces:
   - Open Grafana: http://localhost:3000
   - Go to **Dashboards > Trace Explorer**
   - Click the link to open **Explore** with Tempo.
   - In "Search" tab, find your trace (e.g. by time or missing service name).
   - Click a Trace ID to see the waterfall.

## Troubleshooting

### "Permission Denied" in Tempo Logs
Tempo requires write access to `/tmp/tempo`. We run the container as `user: root` in `docker-compose.observability.yml` to ensure it can write to the volume.

### Missing Traces in Postgres
Run the regression check script:
```bash
python3 scripts/observability/verify_postgres_persistence.py
```
If this fails (0 spans found), check:
- `otel-worker` logs.
- `otel.ingestion_queue` table in `agent_control` DB.
- Ensure `otel-collector` is running and properly connected to `otel-worker`.

### Missing Traces in Tempo
- Ensure `otel-collector` logs don't show connection errors to `tempo:4317`.
- Ensure Tempo is healthy: `docker ps | grep tempo`.
