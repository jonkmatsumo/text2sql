# syntax=docker/dockerfile:1.5
FROM python:3.12-slim

WORKDIR /app

# Install build dependencies for hnswlib (system deps rarely change)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definition first (cache invalidation layer)
COPY common/pyproject.toml ./common/
COPY schema/pyproject.toml ./schema/
COPY dal/pyproject.toml ./dal/
COPY ingestion/pyproject.toml ./ingestion/
COPY mcp-server/pyproject.toml ./mcp-server/

# Create dummy source to allow pip install to work for dependencies
RUN mkdir -p src/common && touch src/common/__init__.py
RUN mkdir -p schema/src/schema && touch schema/src/schema/__init__.py
RUN mkdir -p dal/src/dal && touch dal/src/dal/__init__.py
RUN mkdir -p ingestion/src/ingestion && touch ingestion/src/ingestion/__init__.py
RUN mkdir -p mcp-server/src/mcp_server && touch mcp-server/src/mcp_server/__init__.py

# Install packages with cache mount (so repeat builds are fast)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install ./common ./schema ./dal ./ingestion ./mcp-server

# Copy source code (this changes frequently)
COPY src/common ./src/common
COPY schema/src/ ./schema/src/
COPY dal/src/ ./dal/src/
COPY ingestion/src/ ./ingestion/src/
COPY mcp-server/src/ ./mcp-server/src/

# Re-install in editable mode
# This ensures that imports resolve correctly and bind mounts work.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e ./common -e ./schema -e ./dal -e ./ingestion -e ./mcp-server

# Download SpaCy model for linguistic canonicalization
RUN python -m spacy download en_core_web_sm

# Expose SSE port
EXPOSE 8000

# Run the MCP server
CMD ["python", "mcp-server/src/mcp_server/main.py"]
