# Ingestion Wizard Runbook (Dev)

## Overview
The Ingestion Wizard allows interactive generation of NLP patterns (synonyms) from database columns.

## Access
1. Navigate to `/ops` (System Operations).
2. Click "NLP Patterns" tab.
3. Click "New Ingestion Run (Wizard)".

## Workflow

### 1. Analyze Source
- Click "Analyze Source".
- The system scans tables for enum-like columns (low cardinality).
- **Result:** A list of candidates (Table.Column).

### 2. Enrich (Review Candidates)
- Select the candidates you want to generate synonyms for.
- Click "Generate Suggestions".
- **Result:** LLM generates synonyms for the distinct values in selected columns.

### 3. Review Suggestions
- Review the generated synonyms.
- Toggle "Accept" off for bad suggestions.
- Add new synonyms manually using the input field.
- **Note:** New synonyms are tagged "NEW".

### 4. Commit & Hydrate
- Click "Next: Confirmation".
- Review the summary.
- Click "Commit & Hydrate".
- **Result:**
  - Patterns are inserted into `nlp_patterns` table.
  - `hydrate_schema` job is triggered to update the graph index.

## Troubleshooting

### Stuck Analysis
- Check `nlp_pattern_runs` table for runs with status `RUNNING` or `AWAITING_REVIEW`.
- Check logs for `ui-api-gateway`.

### Hydration Failed
- If the final step shows "Failed", check the `ops_jobs` table for the error message.
- You can retry hydration manually from the "Schema" tab in System Operations.

## Database State
- **Runs:** `nlp_pattern_runs` (stores state, config snapshot)
- **Patterns:** `nlp_patterns` (final patterns)
