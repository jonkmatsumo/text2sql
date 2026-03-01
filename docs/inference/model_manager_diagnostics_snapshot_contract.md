# ModelManager Diagnostics Snapshot Contract (v4)

This document defines the operator-facing contract for the `model_manager` diagnostics snapshot emitted by `build_operator_diagnostics()`.

## Location

- Builder: `src/common/config/model_manager_operability.py`
- Surface: `src/common/config/diagnostics.py` (`model_manager` key)

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
