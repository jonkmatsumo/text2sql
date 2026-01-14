# Ingestion Hardening

This document details the hardening features implemented in the OTEL Worker to ensure robustness, stability, and observability under load.

## Architecture Overview

The ingestion pipeline follows a **Staged Event-Driven Architecture**:

1.  **Ingress (FastAPI)**: Receives OTLP payloads.
    *   **Rate Limiting**: Token-bucket protection before any processing.
    *   **Staging Check**: Rejects/Drops traffic if the persistent staging queue is overloaded.
    *   **Persistence**: Writes raw payload to Postgres `ingestion_queue` (Status: `pending`).
2.  **Processing (Background Workers)**:
    *   **Polling**: Worker threads poll `ingestion_queue` for `pending` items.
    *   **Backpressure**: Polling pauses if the in-memory processing buffer is full.
    *   **Batching**: Groups items into optimal write batches (minio uploads + postgres implementation).
    *   **Completion**: Marks items as `complete` (removed from queue log) or `failed` (triggering retry backoff) in Postgres.

## Configuration settings

All settings are configured via environment variables (prefix `OTEL_` optional if using `.env` loader, but keys map to `Settings` class).

### 1. Rate Limiting (Pre-Staging)
Protect the database insertion path from traffic spikes.
*   `ENABLE_RATE_LIMITING`: `true` / `false` (default: `false`)
*   `RATE_LIMIT_RPS`: Requests per second allowed (default: `100.0`)
*   `RATE_LIMIT_BURST`: Burst capacity (default: `200`)

### 2. Ingress Overflow (Staging Capacity)
Protect the persistent queue from growing indefinitely.
*   `STAGING_MAX_BACKLOG`: Max number of `pending` items in DB before rejecting new requests (default: `1000`).
*   `OVERFLOW_POLICY`: Action to take when saturated:
    *   `reject`: Return `429 Too Many Requests`.
    *   `drop`: Return `202 Accepted` but silently discard (load shedding).
    *   `sample`: Probabilistically accept `OVERFLOW_SAMPLE_RATE` (0.0-1.0).

### 3. Processing Backpressure (Worker Capacity)
Protect the worker memory from OOM during persistent storage slowdowns.
*   `PROCESSING_QUEUE_MAX_DEPTH`: Max items in worker memory buffer (default: `100`).
    *   If full, polling pauses until space clears.

### 4. Batching
Optimize write throughput.
*   `BATCH_MAX_SIZE`: Max items per DB transaction (default: `25`).
*   `BATCH_FLUSH_INTERVAL_MS`: Max time to wait before flushing a partial batch (default: `200`).

## Observability Signals

The worker emits structured JSON logs for visibility into overload conditions:

| Event | Reason | Logic |
|---|---|---|
| `rate_limited` | `limit_exceeded` | Token bucket empty. Returns 429. |
| `queue_saturated` | `queue_full_reject` | DB backlog > Limit. Policy=Reject. Returns 429. |
| `load_shedding` | `queue_full_drop_or_sample` | DB backlog > Limit. Policy=Drop/Sample. Returns 202. |
| `processing_paused` | `buffer_full` | Memory buffer > Limit. Polling sleeps. |
| `batch_persist_failed` | `...` | Batch write error. Items marked `failed`. |

## Development

### Stress Testing
A reproducible stress test harness is available:
```bash
# Run CI-safe stress test (verifies logic)
make stress-verify

# Run Load test (aggressive)
make stress-verify MODE=stress
```
