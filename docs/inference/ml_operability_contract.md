# ML Operability Contract (v5)

This is the canonical operator-facing contract for ML operability payloads in this repo.

## Runtime surfaces

- `build_operator_diagnostics()` in `src/common/config/diagnostics.py`
- `get_ml_health_summary()` in `src/common/config/model_manager_operability.py`
- `detect_schema_drift_details(...).to_dict()` in `src/agent/utils/drift_detection.py`

## `diagnostics.ml_health`

`diagnostics.ml_health` always includes the same top-level sections:

- `model`
- `benchmark`
- `drift`
- `feature_coverage`
- `config`

Optional fields are always present and use `null` when unavailable (never omitted).

### `model`

| Field | Type | Presence | Bound |
| --- | --- | --- | --- |
| `state` | `string` | required | `<=32` |
| `active_model_version` | `string` | required | `<=128` |
| `last_reload_status` | `string` | required | `<=32` |
| `last_reload_ts` | `string \| null` | optional | `<=64` |
| `schema_mismatch_detected` | `boolean` | required | n/a |

### `benchmark`

| Field | Type | Presence | Bound |
| --- | --- | --- | --- |
| `enabled` | `boolean` | required | n/a |
| `last_status` | `string \| null` | optional | `<=32` |
| `last_run_ts` | `string \| null` | optional | `<=64` |

### `drift` (summary view under diagnostics)

| Field | Type | Presence | Bound |
| --- | --- | --- | --- |
| `reference_resolution_mode` | `string` | required | enum (`alias`,`stage`,`latest`,`none`) |
| `last_error_code` | `string \| null` | required key | `<=64` |
| `error_code` | `string \| null` | required key | `<=64` |
| `error_message` | `string \| null` | required key | `<=200` |
| `resolution_mode` | `string` | required | enum (`alias`,`stage`,`latest`,`none`) |
| `reference_model_version` | `string \| null` | required key | `<=128` |
| `bucketing_requested` | `boolean \| null` | required key | n/a |
| `bucketing_used` | `boolean \| null` | required key | n/a |

Notes:

- `last_error_code` mirrors `error_code` for compatibility.
- `reference_resolution_mode` mirrors `resolution_mode` for compatibility.

### `feature_coverage`

| Field | Type | Presence | Bound |
| --- | --- | --- | --- |
| `last_ratio` | `number \| null` | required key | clamped to `[0, 1]` |
| `below_threshold` | `boolean \| null` | required key | n/a |

### `config` (strict config visibility)

| Field | Type | Source |
| --- | --- | --- |
| `strict_feature_schema` | `boolean` | `MODEL_MANAGER_STRICT_SCHEMA_MODE` |
| `strict_tuning_resume_validation` | `boolean` | `MODEL_MANAGER_STRICT_RELOAD_MODE` |
| `strict_split_strategy_validation` | `boolean` | `MODEL_MANAGER_DRIFT_STRICT_MODE` |
| `strict_calibration_validation` | `boolean` | `MODEL_MANAGER_CALIBRATION_STRICT_MODE` |
| `strict_schema_mismatch_blocking` | `boolean` | `AGENT_BLOCK_ON_SCHEMA_MISMATCH` |

All `config` values default to `false`.

## Drift detection result contract

`detect_schema_drift_details(...).to_dict()` returns a uniform shape across success and failure modes.

| Field | Type | Presence | Bound |
| --- | --- | --- | --- |
| `missing_identifiers` | `string[]` | required | bounded list in caller/tests |
| `method` | `string` | required | enum from `DriftDetectionMethod` |
| `source` | `string` | required | `regex` or `structured` |
| `last_error_code` | `string \| null` | required key | `<=64` |
| `error_code` | `string \| null` | required key | `<=64` |
| `error_message` | `string \| null` | required key | `<=200` |
| `reference_resolution_mode` | `string` | required | enum (`alias`,`stage`,`latest`,`none`) |
| `resolution_mode` | `string` | required | enum (`alias`,`stage`,`latest`,`none`) |
| `reference_model_version` | `string \| null` | required key | `<=128` |
| `reference_available` | `boolean` | required | n/a |
| `reference_selection_source` | `string` | required | enum (`alias`,`stage`,`latest`,`none`) |
| `bucketing_requested` | `boolean \| null` | required key | n/a |
| `bucketing_used` | `boolean \| null` | required key | n/a |

Notes:

- `last_error_code` mirrors `error_code` for compatibility.
- `reference_resolution_mode` mirrors `resolution_mode` for compatibility.

## Current runtime examples

### `diagnostics.ml_health` (abbreviated)

```json
{
  "model": {
    "state": "idle",
    "active_model_version": "unknown",
    "last_reload_status": "not_loaded",
    "last_reload_ts": null,
    "schema_mismatch_detected": false
  },
  "benchmark": {
    "enabled": false,
    "last_status": null,
    "last_run_ts": null
  },
  "drift": {
    "reference_resolution_mode": "none",
    "last_error_code": null,
    "error_code": null,
    "error_message": null,
    "resolution_mode": "none",
    "reference_model_version": null,
    "bucketing_requested": null,
    "bucketing_used": null
  },
  "feature_coverage": {
    "last_ratio": null,
    "below_threshold": null
  },
  "config": {
    "strict_feature_schema": false,
    "strict_tuning_resume_validation": false,
    "strict_split_strategy_validation": false,
    "strict_calibration_validation": false,
    "strict_schema_mismatch_blocking": false
  }
}
```

### Drift result success shape

```json
{
  "missing_identifiers": [],
  "method": "ast",
  "source": "regex",
  "last_error_code": null,
  "error_code": null,
  "error_message": null,
  "reference_resolution_mode": "latest",
  "resolution_mode": "latest",
  "reference_model_version": null,
  "reference_available": true,
  "reference_selection_source": "latest",
  "bucketing_requested": null,
  "bucketing_used": null
}
```

### Drift result no-reference shape

```json
{
  "missing_identifiers": [],
  "method": "ast",
  "source": "regex",
  "last_error_code": "no_reference_model",
  "error_code": "no_reference_model",
  "error_message": "Reference model unavailable for drift check",
  "reference_resolution_mode": "none",
  "resolution_mode": "none",
  "reference_model_version": null,
  "reference_available": false,
  "reference_selection_source": "none",
  "bucketing_requested": null,
  "bucketing_used": null
}
```
