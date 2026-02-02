# Schema Type Normalization (Display-Only)

## Purpose

Provide a display-only mapping for schema types across providers so RAG
and UI surfaces show consistent, human-friendly labels. This **must not**
influence query generation or execution.

## Sample Type Inputs (Observed/Expected)

- Postgres/Redshift: `int4`, `int8`, `numeric`, `varchar`, `text`, `bool`,
  `timestamp`, `timestamptz`, `date`, `json`, `jsonb`
- MySQL: `int`, `bigint`, `decimal`, `varchar`, `text`, `tinyint(1)`,
  `datetime`, `timestamp`, `date`, `json`
- SQLite: `INTEGER`, `TEXT`, `REAL`, `NUMERIC`, `BLOB`
- Snowflake: `NUMBER`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP_NTZ`
- BigQuery: `INT64`, `FLOAT64`, `NUMERIC`, `STRING`, `BOOL`, `TIMESTAMP`, `DATE`, `JSON`
- Athena: `int`, `bigint`, `double`, `varchar`, `boolean`, `timestamp`, `date`, `json`
- Databricks: `INT`, `BIGINT`, `DOUBLE`, `DECIMAL`, `STRING`, `BOOLEAN`, `TIMESTAMP`, `DATE`, `JSON`
- DuckDB: `INTEGER`, `BIGINT`, `DOUBLE`, `DECIMAL`, `VARCHAR`, `BOOLEAN`, `TIMESTAMP`, `DATE`, `JSON`
- ClickHouse: `Int32`, `Int64`, `Float64`, `Decimal`, `String`, `Bool`, `DateTime`, `Date`, `JSON`

## Proposed Display-Only Mapping

| Canonical Label | Example Source Types |
| --- | --- |
| `int` | `int4`, `int`, `INTEGER`, `INT32` |
| `bigint` | `int8`, `bigint`, `BIGINT`, `INT64` |
| `float` | `float`, `double`, `real`, `FLOAT64` |
| `decimal` | `numeric`, `decimal`, `NUMBER` |
| `string` | `varchar`, `text`, `string` |
| `boolean` | `bool`, `boolean`, `tinyint(1)` |
| `timestamp` | `timestamp`, `timestamptz`, `TIMESTAMP_NTZ` |
| `date` | `date` |
| `json` | `json`, `jsonb` |
| `binary` | `bytea`, `blob` |

## Non-Goals

- No semantic coercion or casting.
- No changes to SQL generation.
- No cross-provider compatibility promises.
