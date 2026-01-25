# text2sql-synth

Deterministic synthetic data generation for text2sql testing and development.

## Overview

This package provides tooling to generate reproducible synthetic datasets for testing text-to-SQL systems. It is designed to be **fully isolated** from other packages in this monorepo and does not share dependencies or code with `mcp-server/`, `agent/`, or `streamlit/`.

## Key Goals

1. **Determinism**: Given the same configuration and seed, the package always produces identical output. This enables reproducible test fixtures and consistent CI behavior.

2. **Isolation**: This package is standalone. It defines its own schema representations and does not import from sibling packages.

3. **Flexibility**: Supports multiple output formats and database loaders for integration testing.

## Installation

```bash
cd synthetic-data
pip install -e .
```

## CLI Usage

The package provides a `text2sql-synth` CLI with the following subcommands:

### Generate synthetic data

```bash
text2sql-synth generate --config <path-to-config.yaml> --out <output-dir>
```

Generates synthetic data based on a configuration file and writes output to the specified directory.

### Validate a manifest

```bash
text2sql-synth validate --manifest <path-to-manifest.json>
```

Validates that a generated manifest is well-formed and internally consistent.

### Load data into PostgreSQL

```bash
text2sql-synth load-postgres --manifest <path-to-manifest.json> --dsn <postgres-dsn>
```

Loads generated data from a manifest into a PostgreSQL database.

## Package Structure

```
synthetic-data/
├── pyproject.toml
├── README.md
└── src/
    └── synthetic_data_gen/
        ├── __init__.py
        ├── cli.py           # CLI entrypoint
        ├── config.py        # Configuration parsing
        ├── context.py       # Generation context (seeds, state)
        ├── orchestrator.py  # Main generation orchestration
        ├── export.py        # Output serialization
        ├── validate.py      # Manifest validation
        ├── loaders/
        │   └── postgres.py  # PostgreSQL loader
        └── util/
            ├── hashing.py   # Deterministic hashing utilities
            └── manifest.py  # Manifest I/O utilities
```

## Development

This package uses a src-layout. Run tests from the `synthetic-data/` directory:

```bash
pytest
```
