# Investigation Report: Trace Inspection UX After Instrumentation Parity

**Date:** 2026-01-16
**Branch:** feature/agent-persistence-linkage
**Status:** Investigation Complete

---

## Executive Summary

This investigation evaluates the feasibility of providing an MLflow-comparable trace inspection experience using Grafana, OTEL Worker APIs, and existing Postgres storage. The findings indicate that **Grafana sequence tables are fully achievable** with existing infrastructure, **waterfall visualization is feasible with limitations**, and **payload drill-down can be implemented via link-outs** to the OTEL Worker API.

**Key Recommendations:**
1. Build three new Grafana panels: Trace Search, Ordered Span Sequence, and Payload Links
2. Use State Timeline panel for waterfall visualization (with caveats)
3. Implement link-out strategy to OTEL Worker `/api/v1/traces/{trace_id}/spans` for payload inspection
4. Surface `trace_id` in Streamlit Admin UI with automatic deep-links

---

## 1. API Capability Map

### Available OTEL Worker Endpoints

| Endpoint | Method | Purpose | Pagination | Filters |
|----------|--------|---------|------------|---------|
| `/api/v1/traces` | GET | List traces | limit=1-200, offset | service, trace_id, start_time_gte/lte, order |
| `/api/v1/traces/{trace_id}` | GET | Trace detail | N/A | include=attributes |
| `/api/v1/traces/{trace_id}/spans` | GET | Spans for trace | limit=1-500, offset | include=attributes |
| `/api/v1/traces/{trace_id}/raw` | GET | Raw OTLP blob | N/A | N/A |
| `/healthz` | GET | Health check | N/A | N/A |

### Response Field Coverage

**Trace List Response:**
```json
{
  "trace_id": "hex-128-bit",
  "service_name": "string",
  "start_time": "ISO8601",
  "end_time": "ISO8601",
  "duration_ms": "integer",
  "span_count": "integer",
  "status": "OK|ERROR",
  "raw_blob_url": "s3://bucket/path"
}
```

**Span List Response:**
```json
{
  "span_id": "hex-64-bit",
  "trace_id": "hex-128-bit",
  "parent_span_id": "nullable string",
  "name": "string",
  "kind": "SERVER|CLIENT|INTERNAL|...",
  "status_code": "STATUS_CODE_OK|ERROR|UNSET",
  "status_message": "nullable string",
  "start_time": "ISO8601",
  "end_time": "ISO8601",
  "duration_ms": "integer",
  "span_attributes": "object (if include=attributes)",
  "events": "array (if include=attributes)"
}
```

### Span Ordering Behavior

**Current:** Spans are ordered by `start_time ASC` (chronological only).

**Limitation:** No support for parent-child + `event.seq` ordering in API. Tree reconstruction must be done client-side using `parent_span_id` references.

### Raw Blob Access

- **Storage:** MinIO S3-compatible
- **Path Pattern:** `s3://bucket/{environment}/{service_name}/{date}/{trace_id}.json.gz`
- **Access:** Via `/api/v1/traces/{trace_id}/raw` endpoint (decompresses and returns JSON)
- **Alternative:** Direct MinIO access using `raw_blob_url` from trace summary

---

## 2. Postgres Schema Coverage for UX Queries

### Core Tables

**`otel.traces`:**
| Column | Type | Indexed | Notes |
|--------|------|---------|-------|
| trace_id | VARCHAR | PK | Unique identifier |
| service_name | VARCHAR | Yes (composite) | With start_time DESC |
| start_time | TIMESTAMP | Yes (DESC) | Primary sort field |
| end_time | TIMESTAMP | No | |
| duration_ms | BIGINT | No | |
| status | VARCHAR | No | "OK" or "ERROR" |
| span_count | INTEGER | No | |
| tenant_id | VARCHAR | No | **GAP: needs index** |
| interaction_id | VARCHAR | No | **GAP: needs index** |
| trace_attributes | JSON | No | Aggregated span attrs |
| resource_attributes | JSON | No | OTLP resource attrs |
| raw_blob_url | VARCHAR | No | MinIO reference |

**`otel.spans`:**
| Column | Type | Indexed | Notes |
|--------|------|---------|-------|
| span_id | VARCHAR | PK | Unique identifier |
| trace_id | VARCHAR | Yes | Foreign key to traces |
| parent_span_id | VARCHAR | No | **GAP: needs index for tree traversal** |
| name | VARCHAR | No | Span operation name |
| kind | VARCHAR | No | SERVER, CLIENT, INTERNAL, etc. |
| status_code | VARCHAR | No | |
| status_message | VARCHAR | No | |
| start_time | TIMESTAMP | No | |
| end_time | TIMESTAMP | No | |
| duration_ms | BIGINT | No | |
| span_attributes | JSON | No | Custom attributes as JSONB |
| events | JSON | No | Array of span events |

**`otel.trace_metrics`:** (Derived)
- trace_id, service_name, start_time, end_time, duration_ms, has_error

**`otel.stage_metrics`:** (Derived)
- trace_id, stage (router/retrieval/generation/execution/synthesis), duration_ms, has_error

### Attribute Storage Analysis

Span attributes are stored as JSON in `span_attributes` column. Current captured fields:
- `tenant_id` (also extracted to column)
- `interaction_id` (also extracted to column)
- `llm.token_usage.input_tokens` (in JSON only)
- `llm.token_usage.output_tokens` (in JSON only)
- `event.seq` - **Stored in span_attributes JSON** (requires extraction via `span_attributes->>'event.seq'`; may be missing for older traces pre-instrumentation)
- `event.type` - **Stored in span_attributes JSON** (requires extraction via `span_attributes->>'event.type'`)
- `event.name` - **NOT persisted as column** (may be in events array)

### Efficient SQL Extraction Patterns

**Extract JSON attributes:**
```sql
-- Extract specific attribute from span_attributes JSON
SELECT
  span_id,
  span_attributes->>'llm.token_usage.input_tokens' AS input_tokens,
  span_attributes->>'llm.token_usage.output_tokens' AS output_tokens
FROM otel.spans
WHERE trace_id = :trace_id;
```

**Note:** Without GIN indexes, JSON key lookups require full scans. Acceptable for single-trace queries but not for aggregate filtering.

### Indexing Gaps (Flag Only)

| Missing Index | Use Case | Priority |
|---------------|----------|----------|
| `traces(tenant_id, start_time DESC)` | Multi-tenant filtering | HIGH |
| `traces(interaction_id)` | Control-plane correlation | HIGH |
| `spans(parent_span_id)` | Tree traversal | MEDIUM |
| `spans(trace_id, status_code)` | Error filtering | MEDIUM |
| `spans USING GIN(span_attributes)` | Attribute search | LOW |

---

## 3. Grafana Dashboard Design (Must-Have)

### Panel A: Trace Search

**Type:** Table
**Position:** Top (full width)
**Purpose:** Filter and browse traces

**SQL Query:**
```sql
SELECT
  trace_id,
  service_name,
  start_time,
  duration_ms,
  span_count,
  status,
  CASE WHEN status = 'ERROR' THEN true ELSE false END AS has_error
FROM otel.traces
WHERE $__timeFilter(start_time)
  AND ($service = '' OR service_name = $service)
ORDER BY start_time DESC
LIMIT 100
```

**Grafana Variables:**
- `$service`: Query variable from `SELECT DISTINCT service_name FROM otel.traces`

**Column Configuration:**
| Column | Transform | Link |
|--------|-----------|------|
| trace_id | Display as string | Data link to Panel B: `/d/text2sql-trace-detail?var-trace_id=${__value.raw}` |
| start_time | Format as datetime | - |
| duration_ms | Format as ms | - |
| span_count | Integer | - |
| status | Color by value (ERROR=red) | - |

---

### Panel B: Trace Detail - Ordered Span Sequence (Core)

**Type:** Table
**Position:** Full dashboard (separate dashboard or drill-down)
**Purpose:** Show all spans for a trace in logical order

**SQL Query (Chronological with Parent Reference):**
```sql
WITH span_tree AS (
  SELECT
    span_id,
    parent_span_id,
    name,
    kind,
    status_code,
    start_time,
    end_time,
    duration_ms,
    -- Calculate relative offset from trace start
    EXTRACT(EPOCH FROM (start_time - (SELECT MIN(start_time) FROM otel.spans WHERE trace_id = $trace_id))) * 1000 AS offset_ms,
    -- Calculate depth via recursive CTE
    0 AS depth
  FROM otel.spans
  WHERE trace_id = $trace_id
    AND parent_span_id IS NULL

  UNION ALL

  SELECT
    s.span_id,
    s.parent_span_id,
    s.name,
    s.kind,
    s.status_code,
    s.start_time,
    s.end_time,
    s.duration_ms,
    EXTRACT(EPOCH FROM (s.start_time - (SELECT MIN(start_time) FROM otel.spans WHERE trace_id = $trace_id))) * 1000 AS offset_ms,
    st.depth + 1 AS depth
  FROM otel.spans s
  JOIN span_tree st ON s.parent_span_id = st.span_id
  WHERE s.trace_id = $trace_id
)
SELECT
  REPEAT('  ', depth) || name AS span_name,
  kind,
  status_code,
  ROUND(offset_ms::numeric, 1) AS "offset (ms)",
  duration_ms AS "duration (ms)",
  span_id,
  parent_span_id,
  CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 'X' ELSE '' END AS error
FROM span_tree
ORDER BY offset_ms ASC, depth ASC;
```

**Alternative (Simpler, No Recursion):**
```sql
SELECT
  name AS span_name,
  kind,
  status_code,
  ROUND(EXTRACT(EPOCH FROM (start_time - MIN(start_time) OVER())) * 1000, 1) AS "offset (ms)",
  duration_ms AS "duration (ms)",
  span_id,
  COALESCE(parent_span_id, '-') AS parent_span_id,
  CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 'X' ELSE '' END AS error
FROM otel.spans
WHERE trace_id = $trace_id
ORDER BY start_time ASC;
```

**Grafana Variable:**
- `$trace_id`: Text input or passed from Panel A data link

**Column Configuration:**
| Column | Notes |
|--------|-------|
| span_name | Indented by depth (in recursive version) |
| kind | Color coded |
| status_code | Color by value |
| offset (ms) | Relative to trace start |
| duration (ms) | Span duration |
| span_id | Data link to Panel C |
| parent_span_id | Reference display |
| error | Visual indicator |

---

### Panel C: Payload Links

**Type:** Table (extension of Panel B) or Stat panel with links
**Purpose:** Provide access to full payload data

**Implementation Options:**

1. **Add columns to Panel B:**
   - `span_id` column with data link: `http://localhost:4320/api/v1/traces/${trace_id}/spans?include=attributes`

2. **Dedicated link panel:**
```sql
SELECT
  'View All Spans (JSON)' AS link_name,
  'http://localhost:4320/api/v1/traces/' || $trace_id || '/spans?include=attributes' AS url
UNION ALL
SELECT
  'View Raw OTLP Blob' AS link_name,
  'http://localhost:4320/api/v1/traces/' || $trace_id || '/raw' AS url
UNION ALL
SELECT
  'View Trace Summary' AS link_name,
  'http://localhost:4320/api/v1/traces/' || $trace_id || '?include=attributes' AS url;
```

**Column Configuration:**
| Column | Transform |
|--------|-----------|
| link_name | Display text |
| url | Data link (open in new tab) |

---

### Proposed Dashboard Layout

```
+-------------------------------------------------------------+
| Dashboard: Text2SQL Trace Search (text2sql-trace-search)    |
+-------------------------------------------------------------+
| Variables: $service (dropdown), $time_range (time picker)   |
+-------------------------------------------------------------+
| Panel A: Trace Search Table (24 units wide)                 |
| [trace_id] [service] [start_time] [duration] [spans] [err]  |
| -> Click trace_id opens Trace Detail dashboard              |
+-------------------------------------------------------------+

+-------------------------------------------------------------+
| Dashboard: Text2SQL Trace Detail (text2sql-trace-detail)    |
+-------------------------------------------------------------+
| Variables: $trace_id (text input, required)                 |
+--------------------------+----------------------------------+
| Panel: Trace Summary     | Panel: Payload Links             |
| (Stat panels)            | (Link list)                      |
| - Duration               | - View Spans JSON                |
| - Span Count             | - View Raw OTLP                  |
| - Error Count            | - View in API                    |
+--------------------------+----------------------------------+
| Panel B: Ordered Span Sequence (24 units wide)              |
| [indent+name] [kind] [status] [offset] [duration] [error]   |
| -> Optional: Click span_id for API link to span attributes  |
+-------------------------------------------------------------+
| Panel: Waterfall (State Timeline) - Optional                |
| [Visual representation of span timing]                      |
+-------------------------------------------------------------+
```

---

## 4. Waterfall Chart Feasibility Assessment

### Evaluation: State Timeline Panel

**Grafana's State Timeline** can visualize spans as horizontal bars over time, but with significant limitations for trace waterfall use cases.

**Requirements for State Timeline:**
- Data must be formatted with timestamps for state transitions
- Each "entity" (span) appears as a row
- State changes are shown as colored regions

**Challenges for Span Waterfall:**
1. **Data Transformation:** Spans must be reformatted from (start_time, end_time) to state transitions
2. **Nesting:** No native parent-child indentation—all spans appear as flat rows
3. **Ordering:** Rows are sorted alphabetically by default; custom ordering requires workarounds
4. **Dense Traces:** Many spans (>50) create visual clutter

### SQL for State Timeline (Experimental)

```sql
-- Transform spans into state timeline format
SELECT
  name AS metric,
  start_time AS "time",
  'RUNNING' AS state
FROM otel.spans
WHERE trace_id = $trace_id

UNION ALL

SELECT
  name AS metric,
  end_time AS "time",
  'COMPLETE' AS state
FROM otel.spans
WHERE trace_id = $trace_id

ORDER BY metric, "time";
```

**State Timeline Configuration:**
- Merge adjacent states: OFF
- Value display: Never (show only bars)
- Row height: Auto
- Align values: Left

### Alternative: Gantt Plugin

The marcusolsson-gantt-panel plugin provides true Gantt visualization but:
- **Status:** Unmaintained (author recommends State Timeline)
- **Installation:** Requires plugin installation (not recommended per constraints)
- **Stability:** Unknown compatibility with Grafana 10.x

### Feasibility Conclusion

| Approach | Verdict | Reasoning |
|----------|---------|-----------|
| State Timeline (built-in) | **Feasible with limitations** | Requires data transformation; no nesting; acceptable for <30 spans |
| Gantt Plugin | **Not recommended** | Unmaintained; requires plugin installation |
| Table with depth + sorting | **Recommended fallback** | Simple, reliable, shows hierarchy via indentation |
| Custom panel | **Defer** | Requires future standalone UI investment |

**Recommendation:**
1. **Primary:** Use table with computed depth + indentation (Panel B above)
2. **Secondary (nice-to-have):** Add State Timeline panel for visual timing overview
3. **Defer:** True waterfall to future standalone UI

---

## 5. Payload Drill-Down UX

### Safe Inline Fields (Small, Show in Grafana Table)

| Field | Source | Display |
|-------|--------|---------|
| span_name | `spans.name` | Always show |
| kind | `spans.kind` | Always show |
| status_code | `spans.status_code` | Always show |
| status_message | `spans.status_message` | Show if not null |
| duration_ms | `spans.duration_ms` | Always show |
| input_tokens | `span_attributes->>'llm.token_usage.input_tokens'` | Show if present |
| output_tokens | `span_attributes->>'llm.token_usage.output_tokens'` | Show if present |

### Link-Out Only Fields (Large, JSON)

| Field | Endpoint | Access Method |
|-------|----------|---------------|
| Full span_attributes | `/api/v1/traces/{trace_id}/spans?include=attributes` | API link (JSON response) |
| Span events array | `/api/v1/traces/{trace_id}/spans?include=attributes` | API link |
| Raw OTLP payload | `/api/v1/traces/{trace_id}/raw` | API link (gzipped JSON) |
| LLM prompts/outputs | In span_attributes or events | API link + manual inspection |

### Recommended UX Flow

```
1. User opens Trace Search dashboard
   -> Filters by time/service
   -> Clicks trace_id

2. Opens Trace Detail dashboard (auto-populated $trace_id)
   -> Views span sequence table (small fields inline)
   -> Sees token counts for LLM spans
   -> Clicks "View Spans JSON" link

3. Opens OTEL Worker API in new tab
   -> Views full JSON response with all attributes
   -> Copies specific payload for debugging

4. (Optional) Clicks "View Raw OTLP"
   -> Downloads/views complete trace blob
```

### API Response Example (for documentation)

```bash
# Fetch spans with attributes
curl "http://localhost:4320/api/v1/traces/{trace_id}/spans?include=attributes"

# Response includes:
{
  "items": [
    {
      "span_id": "abc123",
      "name": "generate_sql_node",
      "span_attributes": {
        "llm.token_usage.input_tokens": 1523,
        "llm.token_usage.output_tokens": 89,
        "llm.model_id": "claude-3-5-sonnet",
        "tenant_id": "acme-corp",
        "interaction_id": "uuid-here"
      },
      "events": [
        {
          "name": "llm.prompt.system",
          "timeUnixNano": "...",
          "attributes": { "content": "You are a SQL assistant..." }
        }
      ]
    }
  ]
}
```

---

## 6. Streamlit Admin UI Link-Out Alignment

### Current State

The Admin Panel (`streamlit_app/pages/Admin_Panel.py`) has an "Observability" view with:
- Grafana dashboard link (hardcoded to `localhost:3001`)
- Manual trace_id input box
- No automatic trace_id display from interactions
- No deep-link from interaction detail to trace

### Recommended Enhancements

**Minimal Plan (No Embedding):**

1. **Display trace_id in Recent Interactions detail view**
   - The `trace_id` is already stored in `query_interactions` table
   - Add read-only field showing trace_id when viewing interaction detail

2. **Add automatic deep-links:**
   ```python
   # In interaction detail view:
   if interaction.trace_id:
       st.link_button(
           "View Trace in Grafana",
           f"http://localhost:3001/d/text2sql-trace-detail?var-trace_id={interaction.trace_id}"
       )
       st.link_button(
           "View Trace API",
           f"http://localhost:4320/api/v1/traces/{interaction.trace_id}"
       )
   ```

3. **Update Observability view:**
   - Replace manual input with pre-filled field from session state
   - Show both Grafana and API links
   - Add time-range hint based on interaction created_at

**Configuration Approach:**
- Use environment variables for base URLs (not hardcoded localhost)
- `GRAFANA_BASE_URL`, `OTEL_WORKER_BASE_URL`

---

## 7. Open Questions Resolved

### Q1: Is `event.seq` persisted as a span attribute in Postgres?

**Answer: YES, in span_attributes JSON.**

The `event.seq` attribute is captured during OTLP ingestion and stored within the `span_attributes` JSONB column. It can be extracted using `span_attributes->>'event.seq'`. Note that older traces pre-instrumentation may not have this attribute.

**Implication:** Span ordering in dashboards can use `start_time` as primary sort key and `event.seq` as secondary sort key for deterministic ordering within the same timestamp.

---

### Q2: Can we compute nesting depth reliably from parent_span_id alone?

**Answer: YES, with recursive CTE.**

```sql
WITH RECURSIVE span_tree AS (
  -- Root spans (no parent)
  SELECT span_id, parent_span_id, 0 AS depth
  FROM otel.spans
  WHERE trace_id = $trace_id AND parent_span_id IS NULL

  UNION ALL

  -- Child spans
  SELECT s.span_id, s.parent_span_id, st.depth + 1
  FROM otel.spans s
  JOIN span_tree st ON s.parent_span_id = st.span_id
  WHERE s.trace_id = $trace_id
)
SELECT * FROM span_tree;
```

**Limitation:** Postgres recursive CTEs can be slow for deeply nested traces (>10 levels) or traces with many spans (>500). Acceptable for typical agent traces (<50 spans, <5 depth).

---

### Q3: What is the fastest query path for "spans for trace_id ordered by nesting + seq"?

**Answer:** Two-phase approach:

1. **Fast path (no nesting):**
   ```sql
   SELECT * FROM otel.spans
   WHERE trace_id = $trace_id
   ORDER BY start_time ASC;
   ```
   - Uses `ix_otel_spans_trace_id` index
   - Returns chronological order (good enough for most cases)

2. **Full path (with nesting):**
   - Use recursive CTE (see Q2)
   - Order by `(depth, start_time)`
   - Apply indentation in display layer

**Recommended:** Default to fast path; offer "Show hierarchy" toggle for recursive view.

---

### Q4: Do we have an API endpoint to fetch raw trace blob by trace_id?

**Answer: YES.**

```
GET /api/v1/traces/{trace_id}/raw
```

Returns gzip-decompressed OTLP JSON payload. Requires trace_id only; internally looks up `raw_blob_url` from traces table and fetches from MinIO.

**Alternative:** Direct MinIO access using URL from `traces.raw_blob_url` field (requires MinIO credentials).

---

### Q5: What payload fields are safe to show inline vs link-out only?

**Answer:** See Section 5 above.

**Safe inline:** span_name, kind, status_code, duration_ms, token counts (extracted from JSON)

**Link-out only:** Full span_attributes, events array, LLM prompts/outputs, raw OTLP blob

---

## 8. Deliverables Summary

### 8.1 Recommended Near-Term UX

| Priority | Item | Effort | Dependencies |
|----------|------|--------|--------------|
| **P0** | Trace Search panel (Panel A) | Low | None |
| **P0** | Trace Detail dashboard with span table (Panel B) | Medium | Panel A |
| **P0** | Payload link buttons (Panel C) | Low | Panel B |
| **P1** | Streamlit trace_id display + deep-links | Low | None |
| **P2** | State Timeline waterfall (experimental) | Medium | Panel B |
| **P3** | Add missing indexes | Low | DBA review |

### 8.2 SQL Query Templates

See Section 3 above for complete queries:
- Trace Search: Filter by time/service, paginated
- Trace Detail: Recursive CTE for depth, offset calculation
- Payload Links: Static link generation

### 8.3 Waterfall Feasibility Decision

**Decision: Feasible with State Timeline (limited), defer true waterfall.**

- State Timeline provides basic timing visualization
- Cannot show parent-child nesting
- Table with indentation is the reliable fallback
- True waterfall requires custom UI (future investment)

### 8.4 Link-Out Strategy

| Target | URL Pattern | Use Case |
|--------|-------------|----------|
| Grafana Trace Detail | `/d/text2sql-trace-detail?var-trace_id={id}` | Visual inspection |
| Spans API | `/api/v1/traces/{id}/spans?include=attributes` | Full attribute access |
| Raw OTLP | `/api/v1/traces/{id}/raw` | Complete payload |
| Trace Summary | `/api/v1/traces/{id}?include=attributes` | Metadata |

### 8.5 Follow-Up Issues to Open

---

#### Issue 1: Create Grafana Trace Search Dashboard

**Title:** `feat(observability): add Grafana trace search dashboard`

**Description:**
Create new Grafana dashboard for trace discovery:
- Table panel with trace list
- Filters: time range, service name, error status
- Columns: trace_id (clickable), service, start_time, duration, span_count, status
- Data link to trace detail dashboard

**Acceptance Criteria:**
- [ ] Dashboard provisioned via JSON in `observability/grafana/dashboards/`
- [ ] Service dropdown variable populated from distinct services
- [ ] Trace ID links open trace detail dashboard
- [ ] Time range filter working with `$__timeFilter`

---

#### Issue 2: Create Grafana Trace Detail Dashboard

**Title:** `feat(observability): add Grafana trace detail dashboard with ordered spans`

**Description:**
Create drill-down dashboard for single trace inspection:
- Text input variable for trace_id
- Summary stat panels (duration, span count, error count)
- Ordered span sequence table with:
  - Indented span names (via recursive CTE)
  - Offset from trace start
  - Duration
  - Status indicator
- Payload link buttons to OTEL Worker API

**Acceptance Criteria:**
- [ ] Dashboard accepts trace_id parameter
- [ ] Spans ordered by depth + start_time
- [ ] Visual indentation shows hierarchy
- [ ] Links to API endpoints functional

---

#### Issue 3: Add State Timeline Waterfall Panel (Experimental)

**Title:** `feat(observability): add experimental waterfall visualization using State Timeline`

**Description:**
Add optional State Timeline panel to trace detail dashboard:
- Transform span data to state timeline format
- Show span timing as horizontal bars
- Document limitations (no nesting, alphabetical row order)

**Acceptance Criteria:**
- [ ] Panel renders span timing visually
- [ ] Documentation notes limitations
- [ ] Panel is collapsible/optional

---

#### Issue 4: Surface trace_id in Streamlit Admin UI

**Title:** `feat(admin): display trace_id and add observability deep-links`

**Description:**
Enhance Admin Panel to show trace information:
- Display trace_id in interaction detail view
- Add "View Trace in Grafana" button
- Add "View Trace API" button
- Use environment variables for base URLs

**Acceptance Criteria:**
- [ ] trace_id visible when viewing interaction details
- [ ] Grafana deep-link opens trace detail dashboard
- [ ] API link opens OTEL Worker trace endpoint
- [ ] URLs configurable via environment

---

#### Issue 5: Add Missing Database Indexes (Flag)

**Title:** `chore(observability): evaluate and add OTEL schema indexes`

**Description:**
Evaluate and implement missing indexes identified in investigation:
- `traces(tenant_id, start_time DESC)` - tenant filtering
- `traces(interaction_id)` - control-plane correlation
- `spans(parent_span_id)` - tree traversal
- `spans(trace_id, status_code)` - error filtering

**Note:** Requires DBA review for production impact.

**Acceptance Criteria:**
- [ ] Indexes evaluated for query patterns
- [ ] Migration created for approved indexes
- [ ] Performance tested in staging

---

#### Issue 6: Persist event.seq Attribute (Future)

**Title:** `feat(observability): capture event.seq span attribute for deterministic ordering`

**Description:**
To support deterministic span ordering beyond timestamp-based sorting:
- Extract `event.seq` from span attributes during ingestion
- Store as indexed column or ensure queryable in JSON
- Update span list query to order by (parent_span_id, event.seq)

**Blocked By:** Instrumentation must emit event.seq attribute

**Acceptance Criteria:**
- [ ] event.seq extracted during OTLP parsing
- [ ] Queryable in Postgres
- [ ] API supports seq-based ordering

---

## 9. References

- [Grafana State Timeline Documentation](https://grafana.com/docs/grafana/latest/panels-visualizations/visualizations/state-timeline/)
- [Grafana Gantt Plugin (Unmaintained)](https://grafana.com/grafana/plugins/marcusolsson-gantt-panel/)
- OTEL Worker API: `observability/otel-worker/src/otel_worker/app.py`
- Schema: `observability/otel-worker/migrations/versions/163d8f446eb9_baseline_otel_schema.py`
- Current Grafana Dashboard: `observability/grafana/dashboards/trace_metrics.json`
- Admin Panel: `streamlit_app/pages/Admin_Panel.py`

---

## 10. Conclusion

This investigation confirms that a meaningful trace inspection experience is achievable using existing Grafana and OTEL Worker infrastructure:

1. **Grafana Sequence Tables:** Fully achievable with recursive CTEs and proper dashboard design
2. **Waterfall Visualization:** Partially achievable via State Timeline; table fallback recommended
3. **Payload Drill-Down:** Link-out strategy to OTEL Worker API is practical and avoids embedding large payloads
4. **Admin UI Integration:** Minimal changes surface trace_id and provide deep-links

The recommended implementation path prioritizes the must-have panels (P0) before experimental visualizations, ensuring developers can inspect traces effectively while deferring complex UI investments.
