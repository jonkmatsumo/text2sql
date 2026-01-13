# syntax=docker/dockerfile:1.5
FROM ghcr.io/mlflow/mlflow:v3.8.1

# Install PostgreSQL client library (psycopg2)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install psycopg2-binary
