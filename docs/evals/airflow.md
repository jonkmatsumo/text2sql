# Airflow Evaluation Orchestration

This document describes the design and usage of the Airflow-based evaluation system.

## Overview

The system allows running automated evaluations of the Text-to-SQL agent against a golden dataset using Apache Airflow for orchestration. It supports:
- **On-demand Runtime**: Airflow stack spins up only when needed (`make eval-airflow-up`).
- **Deterministic Runner**: A standalone library (`airflow_evals`) to run cases and compute metrics.
- **Persistence**: Results stored as JSON artifacts and logged to MLflow (optional).
- **Regression Detection**: Fails the pipeline if accuracy drops or latency spikes.

## Architecture

- **Directory**: `airflow_evals/` (root for evaluation logic and DAGs).
- **Compose**: `docker-compose.evals.yml` (isolated stack).
- **DAG**: `eval_dag.py` triggers the `airflow_evals.runner`.

## Quick Start

1. **Start Airflow Stack**:
   ```bash
   make eval-airflow-up
   ```
   Access Airflow UI at http://localhost:8080 (airflow/airflow).

2. **Trigger Evaluation**:
   - Go to Airflow UI -> DAGs -> `text2sql_evaluation`.
   - Trigger DAG.
   - Or via CLI (inside container):
     ```bash
     airflow dags trigger text2sql_evaluation
     ```

3. **View Results**:
   - Local Artifacts: `airflow_evals/airflow/logs/eval_artifacts/<run_id>/`
   - MLflow: If configured, check the `text2sql_evaluations` experiment.

4. **Stop Stack**:
   ```bash
   make eval-airflow-down
   ```

## Configuration

The runner uses `EvaluationConfig` which can be populated via Airflow params:
- `dataset_path`: Path to golden dataset.
- `limit`: Run only N cases.
- `concurrency`: Async concurrency level.

## Regression Detection

The system automatically compares the current run against a baseline (if configured via `EVAL_BASELINE_PATH`).
Thresholds:
- Accuracy Drop > 5%
- P95 Latency Increase > 20%

If a regression is detected, the `compute_regression` task in the DAG will fail.

## Development

- **Run Tests**:
  ```bash
  pytest airflow_evals/tests/
  ```
- **CLI Usage**:
  ```bash
  python -m airflow_evals.runner --dataset queries/golden.jsonl --output results/
  ```
