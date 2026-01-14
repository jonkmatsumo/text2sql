# syntax=docker/dockerfile:1.5
FROM python:3.12-slim

WORKDIR /app

# Install build dependencies for hnswlib (system deps rarely change)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency definition first (cache invalidation layer)
COPY mcp-server/pyproject.toml ./

# Create dummy source to allow pip install to work for dependencies
# content of src/mcp_server can be empty, but directory structure must match pyproject.toml
RUN mkdir -p src/mcp_server && touch src/mcp_server/__init__.py

# Install packages with cache mount (so repeat builds are fast)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install .

# Copy source code (this changes frequently)
# Overwrites the dummy src
COPY mcp-server/src/ ./src/

# Re-install in editable mode (minimal time, just links source)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e .

# Download SpaCy model for linguistic canonicalization
# (Could be moved up if stable, but usually fine here)
RUN python -m spacy download en_core_web_sm

# Expose SSE port
EXPOSE 8000

# Run the MCP server
CMD ["python", "src/mcp_server/main.py"]
