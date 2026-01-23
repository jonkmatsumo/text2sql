# syntax=docker/dockerfile:1.5
FROM python:3.12-slim

# Install uv provided by Astral
COPY --from=ghcr.io/astral-sh/uv:0.5.21 /uv /uvx /bin/

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace definitions
COPY pyproject.toml uv.lock ./
COPY pyproject/ pyproject/

# Optimize cache: Install external dependencies first
# 1. Export dependencies excluding local workspace packages
# 2. Create venv
# 3. Install external dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --frozen --no-emit-workspace --no-dev --output-file requirements.txt && \
    uv venv && \
    uv pip install -r requirements.txt

# Copy source code
COPY src/ src/

# Sync the project (install workspace members)
# This step is fast because external deps are already installed
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Download SpaCy model
RUN uv pip install pip && uv run python -m spacy download en_core_web_sm

# Expose SSE port
EXPOSE 8000

# Run the MCP server
CMD ["uv", "run", "python", "src/mcp_server/main.py"]
