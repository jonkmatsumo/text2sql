# Airflow Evaluation Stack Deployment Guide

This guide describes how to deploy the on-demand Airflow stack for Golden Dataset evaluations.

## Architecture

The stack consists of:
- **Airflow Webserver**: UI for managing DAGs.
- **Airflow Scheduler**: Orchestrates tasks.
- **Airflow Worker**: Executes evaluation tasks (using Celery).
- **Redis**: Message broker for Celery.
- **PostgreSQL**: Metadata database for Airflow.
- **MinIO**: S3-compatible storage for evaluation artifacts.

## Prerequisites

- Docker and Docker Compose (V2+)
- 4GB+ RAM allocated to Docker

## Setup Instructions

1. **Configure Environment**:
   Copy the example environment file and update secrets:
   ```bash
   cp .env.airflow.example .env.airflow
   ```
   *Edit `.env.airflow` to set strong passwords.*

2. **Initialize Database**:
   ```bash
   docker compose -f docker-compose.evals.yml up airflow-init
   ```

3. **Start the Stack**:
   ```bash
   docker compose -f docker-compose.evals.yml up -d
   ```

## Accessing Services

- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (Default: admin/admin)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (Default: minioadmin/minioadmin)

## Troubleshooting

### Logs
Tail logs for all services:
```bash
docker compose -f docker-compose.evals.yml logs -f
```

### Resetting the environment
To wipe all data and start fresh:
```bash
docker compose -f docker-compose.evals.yml down -v
```

## Security Recommendations

1. **Change all default passwords** in `.env.airflow`.
2. Do not expose 8080 or 9001 to the public internet without a reverse proxy and proper auth.
3. Use the `airflow-internal` network for sensitive internal communications.
