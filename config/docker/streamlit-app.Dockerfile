# syntax=docker/dockerfile:1.5
FROM python:3.12-slim

# Install uv provided by Astral
COPY --from=ghcr.io/astral-sh/uv:0.5.21 /uv /uvx /bin/

WORKDIR /app
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace definitions
COPY pyproject.toml uv.lock ./
COPY pyproject/ pyproject/

# Optimize cache: Install external dependencies first
RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --frozen --no-emit-workspace --no-dev --output-file requirements.txt && \
    uv venv && \
    uv pip install -r requirements.txt

# Copy source code
COPY src/ src/

# Sync the project (install workspace members in editable mode)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit (using uv run)
CMD ["uv", "run", "streamlit", "run", "src/ui/Text_2_SQL_Agent.py", "--server.port=8501", "--server.address=0.0.0.0"]
