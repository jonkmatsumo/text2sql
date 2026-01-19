# Evaluation CI & Baselines

This document describes how the evaluation CI workflow handles metrics gating and regression detection.

## Workflow Overview

The [Golden Dataset Evaluation](.github/workflows/evaluation.yml) workflow:
1. Installs necessary packages (`schema`, `agent`, `mcp-server`).
2. Runs the evaluation suite in `--golden-only` mode.
3. Uploads artifacts to the GitHub Action run.
4. Performs metrics gating (EM Rate < 50% fails).
5. Compares results against established baselines.

## Baselines

Baselines are stored in:
- `evaluation/baselines/synthetic_baseline.json`
- `evaluation/baselines/pagila_baseline.json`

### Promoting a Run to Baseline

To establish or update a baseline:
1. Run the evaluation (either locally or via CI).
2. Inspect the `summary.json` from the evaluation artifacts.
3. Copy the contents of `summary.json` to the relevant baseline file in `evaluation/baselines/`.
4. Set `"baseline_established": true` in the baseline file.
5. Commit and push the changes.

## Regression Thresholds

When `baseline_established` is `true`:
- **FAIL**: If `exact_match_rate` drops more than **0.05** relative to baseline.
- **WARN**: If `avg_structural_score` drops more than **0.10** relative to baseline.
