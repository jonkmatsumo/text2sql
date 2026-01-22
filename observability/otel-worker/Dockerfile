# syntax=docker/dockerfile:1.5
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definition (replicate repo structure for relative paths)
COPY observability/otel-worker/pyproject.toml ./observability/otel-worker/

# Create dummy source for dependency cache
RUN mkdir -p src/otel_worker && touch src/otel_worker/__init__.py

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e ./observability/otel-worker

COPY src/otel_worker ./src/otel_worker
COPY observability/otel-worker/alembic.ini .
COPY observability/otel-worker/migrations/ ./migrations/

EXPOSE 4320

CMD ["uvicorn", "otel_worker.app:app", "--host", "0.0.0.0", "--port", "4320"]
