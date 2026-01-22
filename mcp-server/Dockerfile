# syntax=docker/dockerfile:1.5
FROM python:3.12-slim

WORKDIR /app

# Install build dependencies for hnswlib (system deps rarely change)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definition first (cache invalidation layer)
COPY pyproject/common/pyproject.toml ./pyproject/common/
COPY pyproject/schema/pyproject.toml ./pyproject/schema/
COPY pyproject/dal/pyproject.toml ./pyproject/dal/
COPY pyproject/ingestion/pyproject.toml ./pyproject/ingestion/
COPY mcp-server/pyproject.toml ./mcp-server/

# Create dummy source to allow pip install to work for dependencies
# Configuration is in pyproject/common/pyproject.toml, pointing to ../../src
# So we need to create src/common at the root, AND install from pyproject/common
RUN mkdir -p src/common && touch src/common/__init__.py
RUN mkdir -p src/schema && touch src/schema/__init__.py
RUN mkdir -p src/dal && touch src/dal/__init__.py
RUN mkdir -p src/ingestion && touch src/ingestion/__init__.py
RUN mkdir -p src/mcp_server && touch src/mcp_server/__init__.py

# Install packages with cache mount (so repeat builds are fast)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e ./pyproject/common -e ./pyproject/schema -e ./pyproject/dal -e ./pyproject/ingestion -e ./mcp-server

# Copy source code (this changes frequently)
COPY src/common ./src/common
COPY src/schema ./src/schema
COPY src/dal ./src/dal
COPY src/ingestion ./src/ingestion
COPY src/mcp_server ./src/mcp_server

# Re-install in editable mode
# This ensures that imports resolve correctly and bind mounts work.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e ./pyproject/common -e ./pyproject/schema -e ./pyproject/dal -e ./pyproject/ingestion -e ./mcp-server

# Download SpaCy model for linguistic canonicalization
RUN python -m spacy download en_core_web_sm

# Expose SSE port
EXPOSE 8000

# Run the MCP server
CMD ["python", "src/mcp_server/main.py"]
