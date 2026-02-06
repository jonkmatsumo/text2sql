# Query Target Directory

This directory contains the schema, data, and queries for the **Target Database** (the database the agent queries against).

## Dataset Mode

The default dataset is **synthetic** (financial transactions domain). This is controlled by the `DATASET_MODE` environment variable:

- `DATASET_MODE=synthetic` (default): Uses the synthetic financial dataset with tables like `dim_customer`, `fact_transaction`, `dim_merchant`, etc.

## Required Files

To run the system, you must populate this directory with:

1.  **Schema & Data**:
    *   `01-schema.sql`: DDL for the target database.
    *   `02-data.sql`: DML/Data for the target database.
    *   For synthetic data: Run `scripts/data/generate_synthetic_artifacts.sh` to generate schema and data.

> [!IMPORTANT]
> **FK Constraints**: Foreign key constraints in your schema are **optional but strongly recommended**.
> They drive join discovery for multi-table queries. Without FKs, the agent may generate
> incorrect or incomplete JOIN clauses. A warning will be logged at startup if no FKs are detected.

> [!NOTE]
> **tables.json (Quality-Only)**: The `queries/tables.json` file provides table descriptions
> for schema embeddings. Its absence degrades retrieval quality but does **not** prevent
> the server from running. A warning will be logged at startup if missing.

> [!NOTE]
> **Query Examples (Quality-Only)**: Few-shot query examples in `queries/*.json` improve
> SQL generation accuracy. Their absence does **not** prevent the server from running,
> but generation quality will be degraded. Registry size is logged at startup.

2.  **Queries** (`queries/`):
    *   Place your `.json` or `.sql` query files here for seeding.
    *   See `queries/example.json` for format.

3.  **Patterns** (`patterns/`):
    *   Place SpaCy entity patterns (`.jsonl`) here.
    *   See `patterns/example.jsonl` for format.

4.  **Corpus** (`corpus/`):
    *   Place golden dataset files for testing here.

## Directory Structure
```
database/query-target/
├── 01-schema.sql          (Ignored)
├── 02-data.sql            (Ignored)
├── download_data.sh       (Ignored, script to fetch demo data)
├── queries/
│   ├── example.json       (Tracked)
│   └── ...                (Ignored)
├── patterns/
│   ├── example.jsonl      (Tracked)
│   └── ...                (Ignored)
└── corpus/
    └── ...                (Ignored)
```
