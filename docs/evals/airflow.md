# Airflow Evaluation Architecture Design

## 1. Overview
This document outlines the design for the "On-Demand" Airflow stack used for orchestrating agent evaluations.
The goal is to provide a robust, reproducible evaluation pipeline that does NOT burden the daily local development loop.

## 2. Run Modes
We support two modes of execution:

### A. CLI Mode (Local / Debug)
*   **Command**: `python -m evaluation.runner ...`
*   **Use Case**: Fast inner-loop testing of the evaluation logic or running a specific test case without spinning up Airflow.
*   **Dependencies**: Requires the application stack (agent, DB, etc.) to be runnable or mocked, but does NOT require Airflow.

### B. Orchestrated Mode (Airflow)
*   **Command**: `make eval-airflow-trigger` (or via UI)
*   **Use Case**: Formal evaluation runs, regression testing, history tracking.
*   **Mechanism**: A DAG (`eval_dag`) invokes the CLI runner (via `PythonOperator` or `DockerOperator`) and manages side-effects (regression checks, alerts).

## 3. Architecture & Components

### Directory Structure
```
evaluation/
├── runner/           # The core logic library
│   ├── __main__.py   # CLI entrypoint
│   ├── core.py       # run_evaluation()
│   └── config.py     # Pydantic models
├── airflow/
│   └── dags/         # Mounted to Airflow containers
│       └── eval_dag.py
├── schema/           # JSON schemas for metrics/artifacts
└── tests/            # Tests for runner and DAGs
```

### Infrastructure (On-Demand)
*   **Compose File**: `docker-compose.evals.yml`
*   **Services**:
    *   `airflow-webserver`
    *   `airflow-scheduler`
    *   `airflow-worker` (or LocalExecutor)
    *   `postgres-airflow` (dedicated metadata DB)
*   **Isolation**: These services are NOT part of the default `docker-compose.app.yml`. They are started explicitly via `make eval-airflow-up`.

## 4. Artifacts & Persistence
Every run produces a directory: `artifacts/evals/<run_id>/`

*   `results.json`: Detailed outputs for every test case.
*   `summary.json`: Aggregated metrics (accuracy, latency, etc.).
*   `regression_report.json` (Phase 5): Diff against baseline.

**MLflow Integration**:
*   The runner is responsible for logging `summary.json` metrics to MLflow.
*   Artifacts are uploaded to MLflow Artifact Store.

## 5. Configuration Strategy
*   **EvaluationConfig**: A Pydantic model defining:
    *   `dataset_path`: Path to golden dataset.
    *   `agent_config`: Hash/Pointer to agent configuration.
    *   `concurrency`: Parallelism for runner.
    *   `seed`: For deterministic playback.

This config can be passed as a JSON blob or loaded from a file.
