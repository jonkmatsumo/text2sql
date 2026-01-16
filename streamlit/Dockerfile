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
# 2. Schema deps
COPY schema/pyproject.toml ./schema/
# 3. Ingestion deps
COPY ingestion/pyproject.toml ./ingestion/
# 4. Streamlit app deps
COPY streamlit/pyproject.toml ./streamlit/
# 5. Agent deps
COPY agent/pyproject.toml ./agent/
# 6. MCP Server deps
COPY mcp-server/pyproject.toml ./mcp-server/
# 7. DAL deps
COPY dal/pyproject.toml ./dal/

# Create dummy source files for local dependencies enable caching
# Common expects src/common
RUN mkdir -p common/src/common && touch common/src/common/__init__.py
RUN mkdir -p schema/src/schema && touch schema/src/schema/__init__.py
RUN mkdir -p ingestion/src/ingestion && touch ingestion/src/ingestion/__init__.py
RUN mkdir -p agent/src/agent_core && touch agent/src/agent_core/__init__.py
RUN mkdir -p mcp-server/src/mcp_server && touch mcp-server/src/mcp_server/__init__.py
RUN mkdir -p dal/src/dal && touch dal/src/dal/__init__.py

# Install dependencies for all packages using cache mounts.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install ./common ./schema ./ingestion ./streamlit ./agent ./mcp-server ./dal

# --- SOURCE LAYER ---
# Now copy the actual source code. Changing a .py file will strictly
# invalidate layers below this point.

# Common source
COPY common/src/ ./common/src/

# Schema source
COPY schema/src/ ./schema/src/

# Ingestion source
COPY ingestion/src/ ./ingestion/src/

# Streamlit source
COPY streamlit_app/ ./streamlit_app/
COPY streamlit/tests/ ./streamlit/tests/

# Agent source
COPY agent/src/ ./agent/src/

# MCP Server source
COPY mcp-server/src/ ./mcp-server/src/

# DAL source
COPY dal/src/ ./dal/src/

# --- EDITABLE INSTALL LAYER ---
# Re-install in editable mode so that imports resolve correctly to the /app/x/src locations
# and standard development workflows (bind mounts) work as expected.
# This is very fast as dependencies are already installed.

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e ./common \
    && pip install -e ./schema \
    && pip install -e ./ingestion \
    && pip install -e ./streamlit \
    && pip install -e ./mcp-server \
    && pip install -e ./agent \
    && pip install -e ./dal

# Expose Streamlit port
EXPOSE 8501

# Set working directory to repo root
WORKDIR /app

# Run Streamlit from streamlit_app entrypoint
CMD ["streamlit", "run", "streamlit_app/Text_2_SQL_Agent.py", "--server.port=8501", "--server.address=0.0.0.0"]
