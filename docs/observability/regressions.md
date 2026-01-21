# Regression Detection Specification

## Overview
This document defines the regression detection logic for the Text2SQL observability pipeline. Regressions are computed from aggregated metrics in `otel.trace_metrics` and persisted to `otel.metric_regressions`.

## Supported Regressions

### Metrics
1.  **Latency (`duration_ms`)**:
    -   Target: p50, p90, p99
    -   Unit: Milliseconds
2.  **Error Rate (`has_error`)**:
    -   Target: Percentage (0-100) / Ratio (0.0-1.0)
3.  **Token Usage (`total_tokens`)**:
    -   Target: Average, p95
    -   Unit: Count

### Comparison Modes
1.  **Time-Window vs Time-Window (Rolling)**:
    -   Compare `[now - window, now]` vs `[now - window - offset, now - offset]`.
    -   Example: "Last 30m" vs "Previous 30m".
2.  **Version vs Baseline (Future)**:
    -   Compare `git_sha=Current` vs `git_sha=Baseline`.

## Threshold Policy

### Configuration
-   **Relative Delta (%)**: A percentage increase that triggers a regression (e.g., +20%).
-   **Absolute Delta**: A raw value increase that triggers a regression (e.g., +250ms).
-   **Minimum Sample Size**: The minimum number of traces required in *both* windows to compute a regression (e.g., N=10).

### logic
A regression is flagged (`fail`) if:
1.  `sample_size >= min_samples` AND
2.  `delta_pct >= threshold_pct` AND
3.  `delta_abs >= threshold_abs`

A warning (`warn`) may be flagged for lower thresholds.

## Persistence Schema (`otel.metric_regressions`)

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | UUID | Primary Key |
| `computed_at` | Timestamp | When the check ran |
| `window_start` | Timestamp | Start of candidate window |
| `window_end` | Timestamp | End of candidate window |
| `baseline_window_start` | Timestamp | Start of baseline window |
| `baseline_window_end` | Timestamp | End of baseline window |
| `metric_name` | String | e.g., `latency_p95`, `error_rate` |
| `dimensions` | JSONB | Filter params (e.g., `{"service": "agent"}`) |
| `baseline_value` | Float | Value in baseline window |
| `candidate_value` | Float | Value in candidate window |
| `delta_abs` | Float | `candidate - baseline` |
| `delta_pct` | Float | `(candidate - baseline) / baseline` |
| `status` | String | `pass`, `warn`, `fail` |
| `sample_size` | Integer | Count of traces in candidate window |
| `top_trace_ids` | Array[String] | Trace IDs exemplifying the regression |

## Drill-Down
Records should include `top_trace_ids` (e.g., the slowest 5 traces for a latency regression) to allow direct navigation to Tempo.
