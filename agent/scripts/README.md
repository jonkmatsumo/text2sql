# Agent Scripts

This directory contains **offline evaluation and analysis tooling** that is
**NOT required** for MCP server or agent runtime.

## Evaluation Tooling

### Files

| Script | Purpose |
|--------|---------|
| `run_evaluation.py` | Run agent against golden_dataset test cases |
| `evaluation_metrics.py` | Compute metrics from evaluation results |

### golden_dataset

The `golden_dataset` table (in the control-plane database) is used **exclusively
for evaluation** and is:

- **NOT** referenced by any runtime code (`agent_core`, `mcp_server`, `dal`)
- **NOT** required to start or operate the MCP server or agent
- Used **only** by evaluation scripts to measure answer quality

> [!NOTE]
> If you are deploying the system, you do **not** need to populate
> `golden_dataset`. It is only needed if you want to run automated
> quality evaluations using the scripts in this directory.

## Running Evaluations

```bash
# Ensure the control-plane database has golden_dataset populated
python agent/scripts/run_evaluation.py

# Compute metrics from evaluation results
python agent/scripts/evaluation_metrics.py
```
