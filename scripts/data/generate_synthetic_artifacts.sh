#!/bin/bash
set -e

# Script to generate synthetic data artifacts and place them in the query-target directory.

TARGET_DIR="${1:-database/query-target}"
QUERIES_DIR="${TARGET_DIR}/queries"

echo "Generating synthetic artifacts..."
echo "Target: ${TARGET_DIR}"

# Ensure directories exist
mkdir -p "${QUERIES_DIR}"

# Set PYTHONPATH to include synthetic-data source
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# 1. Generate SQL Schema and Data
echo "Generating SQL schema and data..."
python3 -m synthetic_data_gen.cli export-sql \
    --preset small \
    --out "${TARGET_DIR}" \
    --schema public

# 2. Generate Tables JSON (Summaries)
echo "Generating tables.json..."
python3 -m synthetic_data_gen.cli export-tables-json \
    --out "${QUERIES_DIR}"

# 3. Generate Few-Shot Examples
echo "Generating few-shot examples..."
python3 -m synthetic_data_gen.cli export-examples \
    --out "${QUERIES_DIR}"

echo "✅ Synthetic artifacts generated successfully."
echo "You can now rebuild/restart containers to seed this data."
