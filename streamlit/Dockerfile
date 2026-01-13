FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy project structure (build context is project root)
COPY streamlit/pyproject.toml ./streamlit/
COPY streamlit/Text_2_SQL_Agent.py ./streamlit/
COPY streamlit_app/ ./streamlit_app/
COPY streamlit/pages/ ./streamlit/pages/

# Copy Streamlit config directory
RUN mkdir -p ./streamlit/.streamlit
COPY streamlit/.streamlit/ ./streamlit/.streamlit/

# Copy Agent dependencies and source (needed for agent_core imports)
COPY agent/pyproject.toml ./agent/
COPY agent/src/ ./agent/src/

# Copy MCP Server dependencies (needed for shared config/DAL)
COPY mcp-server/pyproject.toml ./mcp-server/
COPY mcp-server/src/ ./mcp-server/src/

# Install Streamlit dependencies
RUN pip install --no-cache-dir -e ./streamlit

# Install MCP Server dependencies
RUN pip install --no-cache-dir -e ./mcp-server

# Install Agent dependencies (needed for agent_core.graph import)
RUN pip install --no-cache-dir -e ./agent

# Expose Streamlit port
EXPOSE 8501

# Set working directory to streamlit for Text_2_SQL_Agent.py
WORKDIR /app/streamlit

# Run Streamlit
CMD ["streamlit", "run", "Text_2_SQL_Agent.py", "--server.port=8501", "--server.address=0.0.0.0"]
