# syntax=docker/dockerfile:1.5
FROM python:3.12-slim

# Install uv provided by Astral
COPY --from=ghcr.io/astral-sh/uv:0.5.21 /uv /uvx /bin/

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace definitions
COPY pyproject.toml uv.lock ./
COPY pyproject/ pyproject/

# Optimize cache: Install external dependencies first
RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --frozen --no-emit-workspace --no-dev --output-file requirements.txt && \
    uv venv && \
    uv pip install -r requirements.txt

# Copy source code
COPY src/ src/

# Sync the project (install workspace members)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Copy migrations config (if still needed, though src mapping might handle it)
# The original copied them relative to CWD.
# But original had "migrations/" in "observability/otel-worker/".
# I'll check if I need to copy them specifically or if they are in src.
# They are in "observability/otel-worker/migrations".
# This is NOT in src/otel_worker. It is a separate dir.
COPY config/services/otel-worker/alembic.ini .
COPY observability/otel-worker/migrations/ ./migrations/

EXPOSE 4320

CMD ["uv", "run", "uvicorn", "otel_worker.app:app", "--host", "0.0.0.0", "--port", "4320"]
