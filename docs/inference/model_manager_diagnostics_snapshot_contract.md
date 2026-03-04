# ModelManager Diagnostics Snapshot Contract (v5)

This document defines the operator-facing contract for `build_operator_diagnostics()`, including both:

- `model_manager` snapshot (legacy surface)
- `ml_health` summary (bounded operational health surface)

## Location

- Builder: `src/common/config/model_manager_operability.py`
- Surface: `src/common/config/diagnostics.py` (`model_manager` and `ml_health` keys)

## Baseline Required Fields

These fields are always present:

| Field | Type | Description |
| --- | --- | --- |
| `state` | `string` | High-level manager state (for example `idle`, `ready`, `degraded`). |
| `active_model_version` | `string` | Active model version identifier, or `unknown`. |
| `last_reload_status` | `string` | Last reload outcome/state marker (for example `not_loaded`, `warn_only`). |
| `schema_mismatch_detected` | `boolean` | Whether schema mismatch was detected. |

## Optional Fields (Bounded)

These fields are present with a bounded string value or `null`:

| Field | Type | Allowed Values |
| --- | --- | --- |
| `reload_failure_reason` | `string \| null` | `reload_exception`, `build_pipeline_failed`, `service_unavailable` |
| `schema_mismatch_reason` | `string \| null` | `schema_version_mismatch`, `schema_missing_columns`, `schema_missing_tables` |
| `calibration_skip_reason` | `string \| null` | `calibration_disabled`, `insufficient_samples`, `invalid_thresholds` |
| `drift_fallback_reason` | `string \| null` | `drift_fallback_disabled`, `schema_snapshot_unavailable`, `fallback_last_known_good` |

Any non-allowlisted value is normalized to `null`.

## Strict/Warn Semantics

- Strict flags are opt-in and default off:
  - `MODEL_MANAGER_STRICT_RELOAD_MODE=false`
  - `MODEL_MANAGER_STRICT_SCHEMA_MODE=false`
  - `MODEL_MANAGER_CALIBRATION_STRICT_MODE=false`
  - `MODEL_MANAGER_DRIFT_STRICT_MODE=false`
- Warn-only defaults to on:
  - `MODEL_MANAGER_SCHEMA_MISMATCH_WARN_ONLY=true`
- When mismatch is detected in warn-only mode with strict schema mode off, snapshot status is kept non-fatal (`last_reload_status=warn_only`).

## Example Payload

```json
{
  "state": "degraded",
  "active_model_version": "unknown",
  "last_reload_status": "warn_only",
  "schema_mismatch_detected": true,
  "reload_failure_reason": null,
  "schema_mismatch_reason": "schema_version_mismatch",
  "calibration_skip_reason": null,
  "drift_fallback_reason": null
}
```

## ML Health Summary (`diagnostics.ml_health`)

`ml_health` is a compact, bounded payload with stable section keys:

- `model`
- `benchmark`
- `drift`
- `feature_coverage`
- `config`

### `model`

| Field | Type | Notes |
| --- | --- | --- |
| `state` | `string` | Bounded manager state string. |
| `active_model_version` | `string` | Active model version, or `unknown`. |
| `last_reload_status` | `string` | Last reload status. |
| `last_reload_ts` | `string \| null` | Last reload timestamp if available. |
| `schema_mismatch_detected` | `boolean` | Drift/schema mismatch marker. |

### `benchmark`

| Field | Type | Notes |
| --- | --- | --- |
| `enabled` | `boolean` | Whether benchmark signal is enabled/tracked. |
| `last_status` | `string \| null` | Last benchmark status if tracked. |
| `last_run_ts` | `string \| null` | Last benchmark run timestamp if tracked. |

### `drift`

| Field | Type | Notes |
| --- | --- | --- |
| `reference_resolution_mode` | `string` | Compatibility alias for resolution mode. |
| `last_error_code` | `string \| null` | Compatibility alias for drift error code. |
| `error_code` | `string \| null` | Canonical bounded drift reason code. |
| `error_message` | `string \| null` | Short bounded message (`<=200` chars). |
| `resolution_mode` | `string` | One of `alias`, `stage`, `latest`, `none`. |
| `reference_model_version` | `string \| null` | Resolved reference model version, if known. |
| `bucketing_requested` | `boolean \| null` | Bucketing request marker, if tracked. |
| `bucketing_used` | `boolean \| null` | Bucketing usage marker, if tracked. |

Canonical `error_code` intent:

- `no_reference_model`: no usable reference baseline was available.
- `insufficient_reference_samples`: reference baseline exists but sample volume is too low.
- `psi_sparse_buckets`: PSI/similar bucketed drift metric was suppressed due to sparse buckets.
- `none` or `null`: no drift error was detected.

### `feature_coverage`

| Field | Type | Notes |
| --- | --- | --- |
| `last_ratio` | `number \| null` | Bounded to `[0, 1]` if tracked. |
| `below_threshold` | `boolean \| null` | Coverage threshold indicator if tracked. |

### `config` (effective strict-mode visibility)

| Field | Type | Env/Source |
| --- | --- | --- |
| `strict_feature_schema` | `boolean` | `MODEL_MANAGER_STRICT_SCHEMA_MODE` |
| `strict_tuning_resume_validation` | `boolean` | `MODEL_MANAGER_STRICT_RELOAD_MODE` |
| `strict_split_strategy_validation` | `boolean` | `MODEL_MANAGER_DRIFT_STRICT_MODE` |
| `strict_calibration_validation` | `boolean` | `MODEL_MANAGER_CALIBRATION_STRICT_MODE` |
| `strict_schema_mismatch_blocking` | `boolean` | `AGENT_BLOCK_ON_SCHEMA_MISMATCH` |

All `config` values default to `false` unless explicitly enabled.
