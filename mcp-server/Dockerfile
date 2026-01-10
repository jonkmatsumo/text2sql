FROM python:3.12-slim

WORKDIR /app

# Copy dependency files and source code (needed for editable install)
COPY pyproject.toml ./
COPY src/ ./src/

# Install build dependencies for hnswlib
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Download SpaCy model for linguistic canonicalization
RUN python -m spacy download en_core_web_sm

# Expose SSE port
EXPOSE 8000

# Run the MCP server
CMD ["python", "src/mcp_server/main.py"]
