#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Downloading Pagila schema..."
curl -f -s -k https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql -o "${SCRIPT_DIR}/01-schema.sql" || {
    echo "Error: Failed to download schema file. Please check your network connection."
    exit 1
}

echo "Downloading Pagila data..."
curl -f -s -k https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-data.sql -o "${SCRIPT_DIR}/02-data.sql" || {
    echo "Error: Failed to download data file. Please check your network connection."
    exit 1
}

echo "✓ Pagila files downloaded successfully"

