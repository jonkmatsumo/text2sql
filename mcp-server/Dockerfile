# syntax=docker/dockerfile:1.5
FROM python:3.12-slim

WORKDIR /app

# Install build dependencies for hnswlib (system deps rarely change)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definition first (cache invalidation layer)
COPY common/pyproject.toml ./common/
COPY mcp-server/pyproject.toml ./mcp-server/

# Create dummy source to allow pip install to work for dependencies
RUN mkdir -p common/src/common && touch common/src/common/__init__.py
RUN mkdir -p mcp-server/src/mcp_server && touch mcp-server/src/mcp_server/__init__.py

# Install packages with cache mount (so repeat builds are fast)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install ./common ./mcp-server

# Copy source code (this changes frequently)
COPY common/src/ ./common/src/
COPY mcp-server/src/ ./mcp-server/src/

# Re-install in editable mode
# This ensures that imports resolve correctly and bind mounts work.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e ./common -e ./mcp-server

# Download SpaCy model for linguistic canonicalization
RUN python -m spacy download en_core_web_sm

# Expose SSE port
EXPOSE 8000

# Run the MCP server
CMD ["python", "mcp-server/src/mcp_server/main.py"]
