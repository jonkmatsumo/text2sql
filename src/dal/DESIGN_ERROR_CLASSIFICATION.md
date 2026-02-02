# DAL Error Classification (Design)

## Purpose

Define a provider-agnostic error taxonomy for the DAL that **categorizes** errors
without hiding provider semantics, rewriting SQL, or retrying automatically.

Classification is **opt-in** and gated by `DAL_EXPERIMENTAL_FEATURES=true`.

## Principles

- Preserve raw provider exception messages and types.
- Only add a coarse category label for downstream UX and telemetry.
- No retries, no SQL normalization, no automatic fallbacks.

## Taxonomy (Categories Only)

- `auth` — authentication/authorization failures, permission denied.
- `connectivity` — network errors, DNS, connection refused.
- `timeout` — query or network timeouts.
- `resource_exhausted` — out of memory, quota, or capacity errors.
- `syntax` — SQL parse errors or invalid syntax.
- `unsupported` — explicit "feature not supported" responses.
- `transient` — retryable by caller if they choose (no DAL retries).
- `unknown` — default when we cannot classify reliably.

## Mapping Strategy (Provider-Specific)

Each provider maps error codes/messages into the taxonomy **without**
rewriting the exception. The original exception is retained as the cause.

Examples:

- Postgres: `asyncpg.PostgresSyntaxError` → `syntax`
- Snowflake: `ProgrammingError` with "SQL compilation error" → `syntax`
- BigQuery: `BadRequest` with "Invalid query" → `syntax`

## Non-Goals

- No semantic transformation of errors into "portable" messages.
- No cross-provider retries or normalization.
- No automatic downgrade of features.
