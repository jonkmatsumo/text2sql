# Observability Governance

## Overview
This document defines policies for data retention, sampling, and trace management.

## 1. Trace Retention
- **Operational Traces**: Retained for 7 days.
- **Golden Traces**: Retained indefinitely (until manually demoted).
- **Regressions**: Metadata in `otel.metric_regressions` retained indefinitely.

## 2. Sampling Policy
- **Prod**: 10% sampling for successful traces, 100% for error traces. (To be implemented in Collector).
- **Dev/Staging**: 100% sampling.
- **Golden Traces**: Always persisted and never deleted by retention jobs.

## 3. Golden Traces
Golden traces are exemplary traces used for:
- Regression baselines
- Training examples
- Documentation

### Promotion
Traces are promoted via the `promote_golden_trace.py` script.
They are stored in `otel.golden_traces`.

### Schema
Table `otel.golden_traces`:
- `trace_id` (PK, FK to traces)
- `promoted_at` (Timestamp)
- `promoted_by` (User/System)
- `reason` (Text)
- `labels` (JSONB: tags like "latency_benchmark", "correctness_example")
