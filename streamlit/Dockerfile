# syntax=docker/dockerfile:1.5
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# --- DEPENDENCY LAYER ---
# Copy ONLY dependency descriptors for all packages first.
# This ensures that changes to source code files (*.py) do NOT invalidate
# the dependency installation layer.

# 1. Common deps
COPY common/pyproject.toml ./common/
# 2. Streamlit app deps
COPY streamlit/pyproject.toml ./streamlit/
# 3. Agent deps
COPY agent/pyproject.toml ./agent/
# 4. MCP Server deps
COPY mcp-server/pyproject.toml ./mcp-server/

# Create dummy source files for local dependencies enable caching
# Common expects src/common
RUN mkdir -p common/src/common && touch common/src/common/__init__.py
# Agent expects src/agent_core
RUN mkdir -p agent/src/agent_core && touch agent/src/agent_core/__init__.py
# MCP Server expects src/mcp_server
RUN mkdir -p mcp-server/src/mcp_server && touch mcp-server/src/mcp_server/__init__.py

# Install dependencies for all packages using cache mounts.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install ./common ./streamlit ./agent ./mcp-server

# --- SOURCE LAYER ---
# Now copy the actual source code. Changing a .py file will strictly
# invalidate layers below this point.

# Common source
COPY common/src/ ./common/src/

# Streamlit source
COPY streamlit_app/ ./streamlit_app/
COPY streamlit/tests/ ./streamlit/tests/

# Agent source
COPY agent/src/ ./agent/src/

# MCP Server source
COPY mcp-server/src/ ./mcp-server/src/

# --- EDITABLE INSTALL LAYER ---
# Re-install in editable mode so that imports resolve correctly to the /app/x/src locations
# and standard development workflows (bind mounts) work as expected.
# This is very fast as dependencies are already installed.

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e ./common \
    && pip install -e ./streamlit \
    && pip install -e ./mcp-server \
    && pip install -e ./agent

# Expose Streamlit port
EXPOSE 8501

# Set working directory to repo root
WORKDIR /app

# Run Streamlit from streamlit_app entrypoint
CMD ["streamlit", "run", "streamlit_app/Text_2_SQL_Agent.py", "--server.port=8501", "--server.address=0.0.0.0"]
