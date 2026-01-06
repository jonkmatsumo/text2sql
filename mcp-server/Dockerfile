FROM python:3.12-slim

WORKDIR /app

# Copy dependency files and source code (needed for editable install)
COPY pyproject.toml ./
COPY src/ ./src/

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Expose SSE port
EXPOSE 8000

# Run the MCP server
CMD ["python", "src/main.py"]
