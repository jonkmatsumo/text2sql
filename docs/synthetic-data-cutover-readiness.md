# Synthetic Data Cutover Readiness Analysis: Pagila Deprecation

> **Investigation Date**: 2026-01-17  
> **Scope**: Read-only code inspection  
> **Verdict**: ❌ **NOT READY** — Major domain mismatch and extensive Pagila dependencies remain

---

## Executive Summary

This investigation determined that a **complete cutover from Pagila to synthetic data is NOT immediately possible** due to:

1. **Fundamental Domain Mismatch**: The synthetic-data package generates a **financial transactions domain** (customers, accounts, merchants, transactions), while Pagila is a **film rental domain** (films, actors, rentals, stores). These share almost no semantic overlap.

2. **Extensive Hard-Coded Dependencies**: 27+ references to "pagila" in code, including default database names, domain-specific patterns, table whitelists, and control-plane seed data.

3. **Missing Query-Target Contract Artifacts**: Synthetic-data does not produce the artifacts required by the query-target contract (`01-schema.sql`, `02-data.sql`, `tables.json`, query examples).

---

## 1. Pagila Dependency Inventory (Current-State)

### 1.1 Hard-Blocking: Default Database Name (`DB_NAME=pagila`)

| File | Line | Classification | Recommended Action |
|------|------|----------------|-------------------|
| `dal/src/dal/database.py` | 35 | **Hard-blocking** | Parameterize or change default |
| `mcp-server/src/mcp_server/config/database.py` | 35 | **Hard-blocking** | Parameterize or change default |
| `agent/src/agent_core/retriever.py` | 24 | **Hard-blocking** | Parameterize or change default |
| `scripts/run_migrations_v2.py` | 26 | **Hard-blocking** | Parameterize or change default |
| `agent/scripts/evaluation_metrics.py` | 17 | **Soft degradation** | Parameterize |
| `agent/scripts/run_evaluation.py` | 25, 62, 210 | **Soft degradation** | Parameterize |

### 1.2 Hard-Blocking: Docker Compose Hardcoded Values

| File | Line | Reference | Classification | Recommended Action |
|------|------|-----------|----------------|-------------------|
| `docker-compose.infra.yml` | 8 | `container_name: pagila_db` | **Hard-blocking** | Rename to `query_target_db` |
| `docker-compose.infra.yml` | 12 | `POSTGRES_DB: pagila` | **Hard-blocking** | Parameterize via env var |
| `docker-compose.infra.yml` | 21 | `pg_isready -d pagila` | **Hard-blocking** | Use variable |
| `docker-compose.app.yml` | 16, 49, 95 | `DB_NAME: pagila` (3 services) | **Hard-blocking** | Use `${DB_NAME:-pagila}` |

### 1.3 Hard-Blocking: Domain-Specific Table Whitelist

| File | Line | Description | Classification | Recommended Action |
|------|------|-------------|----------------|-------------------|
| `agent/src/agent_core/validation/policy_enforcer.py` | 18-34 | `ALLOWED_TABLES` hardcoded with Pagila tables: `customer`, `rental`, `payment`, `staff`, `inventory`, `film`, `actor`, `address`, `city`, `country`, `category`, `language`, `film_actor`, `film_category`, `store` | **Hard-blocking (Security)** | Must load from env/config/introspection |

> [!CAUTION]
> The `ALLOWED_TABLES` whitelist is a **security control**. If unchanged and synthetic-data tables are used, all queries will fail policy validation.

### 1.4 Soft Degradation: Domain-Specific Canonicalization Patterns

| File | Lines | Description | Classification | Recommended Action |
|------|-------|-------------|----------------|-------------------|
| `mcp-server/src/mcp_server/services/canonicalization/dependency_patterns.py` | 6-8 | Explicitly documents patterns are "tuned for FILM/MOVIE domain schema (Pagila)" | **Soft degradation** | Replace with domain-agnostic or financial patterns |
| `mcp-server/src/mcp_server/services/canonicalization/dependency_patterns.py` | 16-119 | RATING patterns match `movie`, `film`, `G`, `PG`, `PG-13`, `R`, `NC-17` | **Soft degradation** | Replace entirely |
| `mcp-server/src/mcp_server/services/canonicalization/spacy_pipeline.py` | 208, 322-328 | Hardcoded `domain_assumption=film_schema`, entity detection for `movie`, `film` | **Soft degradation** | Parameterize domain |
| `mcp-server/src/mcp_server/services/cache/constraint_extractor.py` | 53, 122-123 | Entity patterns for `film`, `movie` | **Soft degradation** | Replace |
| `mcp-server/src/mcp_server/services/cache/intent_signature.py` | 92-97, 114 | Intent builders reference `film`, `movie`, `actor` | **Soft degradation** | Replace |

### 1.5 Control-Plane Artifacts (Pagila-Specific Seed Data)

| File | Lines | Description | Classification | Recommended Action |
|------|-------|-------------|----------------|-------------------|
| `database/control-plane/06-row-policies.sql` | 22-30 | Seeds row policies for `customer`, `rental`, `payment`, `staff`, `inventory` with `store_id` tenant column | **Soft degradation** | Replace with synthetic-data policies (if RLS required) |

### 1.6 Documentation & UX (Pagila Mentions)

| File | Lines | Description | Classification | Recommended Action |
|------|-------|-------------|----------------|-------------------|
| `README.md` | 97, 115, 124 | Mermaid diagram references `PagilaDB` | **Documentation** | Update diagram |
| `database/query-target/README.md` | 14 | Mentions Pagila as "default demo" | **Documentation** | Update or remove |
| `.env.example` | 43 | `DB_NAME=pagila` | **Documentation** | Change default or document choice |

### 1.7 Tests with Pagila Assumptions

| Test File | Lines | Description | Classification | Recommended Action |
|-----------|-------|-------------|----------------|-------------------|
| `agent/tests/test_retriever.py` | 32, 48, 117 | Sets `DB_NAME=pagila`, expects `pagila` in connection string | **Test-only** | Parameterize |
| `streamlit_app/tests/test_streamlit_agent.py` | 44-63, 99-112 | References `films` in mock data | **Test-only** | Update mocks if domain changes |
| `streamlit_app/tests/test_admin.py` | 93-108 | References `film`, `actor` queries | **Test-only** | Update mocks if domain changes |
| `agent/tests/test_graph.py` | 49, 61, 139, 157, etc. | SQL referencing `film`, `films` | **Test-only** | Update fixtures |
| `agent/tests/test_generate_schema.py` | 50-74 | Schema context referencing `film`, `actor` | **Test-only** | Update fixtures |

---

## 2. Synthetic Data Coverage vs Query-Target Contract

### 2.1 Query-Target Contract Requirements

From `database/query-target/README.md`:

| Requirement | Status | Notes |
|-------------|--------|-------|
| `01-schema.sql` | ❌ **Missing** | Synthetic-data generates CSV/Parquet, not SQL DDL |
| `02-data.sql` | ❌ **Missing** | Synthetic-data generates CSV/Parquet, not SQL DML |
| FK Constraints | ⚠️ **Partial** | `schema.py` defines `DEPENDENCIES` but no FK DDL emitted |
| `tables.json` | ❌ **Missing** | No table description generator in synthetic-data |
| `queries/*.json` | ❌ **Missing** | No few-shot example generator |
| `patterns/*.jsonl` | ❌ **Missing** | No SpaCy pattern generator for financial domain |
| Golden dataset | ⚠️ **Partial** | `golden/mvp_digest.txt` exists but unclear format |

### 2.2 Synthetic-Data Schema Analysis

From `synthetic-data/src/text2sql_synth/schema.py`:

**Tables generated** (17 tables):
- Dimensions: `dim_time`, `dim_institution`, `dim_address`, `dim_customer`, `dim_merchant`, `dim_account`, `dim_counterparty`, `dim_customer_scd2`
- Facts: `fact_transaction`, `fact_payment`, `fact_refund`, `fact_dispute`
- Events: `event_login`, `event_device`, `event_account_status_change`, `event_rule_decision`, `event_account_balance_daily`
- Bridge: `bridge_customer_address`

**Domain**: Financial transactions (payments, accounts, merchants, disputes, fraud detection)

> [!IMPORTANT]
> The synthetic-data domain is **completely different** from Pagila. This is not a drop-in replacement—it represents an entirely different query space with different user questions.

### 2.3 Gap Summary

| Subsystem | Pagila Works? | Synthetic Works? | Gap |
|-----------|---------------|------------------|-----|
| Compose boot | ✅ | ❌ | No SQL artifacts |
| Schema seeding | ✅ | ❌ | No DDL/DML output |
| Graph hydration | ✅ | ⚠️ | Generators exist but not integrated |
| RAG retrieval | ✅ | ❌ | No `tables.json`, no examples |
| Canonicalization | ✅ | ❌ | Patterns hardcoded for film domain |
| Policy enforcement | ✅ | ❌ | Whitelist hardcoded |
| Evaluation | ✅ | ❌ | Golden dataset unknown format |

---

## 3. Cutover Phasing Analysis

### Phase A: Synthetic Data Usable (Core Runtime Unblocked)

**Goal**: System boots and runs with synthetic-data without crashing (quality degraded).

**Required Changes**:

| Category | Change | Effort |
|----------|--------|--------|
| **Code** | Parameterize `DB_NAME` default (remove `pagila` literal) | Low |
| **Code** | Load `ALLOWED_TABLES` dynamically from schema introspection | Medium |
| **Infra** | Parameterize `docker-compose.infra.yml` POSTGRES_DB | Low |
| **Infra** | Parameterize `docker-compose.app.yml` DB_NAME vars | Low |
| **Data** | Add `text2sql-synth export-sql` CLI to emit `01-schema.sql`, `02-data.sql` | Medium |

**Subsystems Unblocked**: Compose boot, basic seeding, query execution

**Pagila Dependencies Removable**: DB_NAME hardcoding

---

### Phase B: Feature Parity for RAG + Registry

**Goal**: Semantic search and caching work with same quality as Pagila.

**Required Changes**:

| Category | Change | Effort |
|----------|--------|--------|
| **Data** | Generate `tables.json` with financial domain descriptions | Medium |
| **Data** | Create few-shot query examples for financial domain | Medium |
| **Code** | Create financial-domain SpaCy patterns (or make patterns data-driven) | High |
| **Code** | Update `intent_signature.py` to be domain-agnostic | Medium |
| **Code** | Update `constraint_extractor.py` entity patterns | Medium |

**Subsystems Unblocked**: RAG retrieval, semantic caching, canonicalization

**Pagila Dependencies Removable**: All canonicalization patterns, intent signature logic

---

### Phase C: Pagila Removal (Clean Deprecation)

**Goal**: Remove all Pagila references; synthetic-data is sole default.

**Required Changes**:

| Category | Change | Effort |
|----------|--------|--------|
| **Docs** | Update README architecture diagram | Low |
| **Docs** | Update `database/query-target/README.md` | Low |
| **Docs** | Update `.env.example` DB_NAME documentation | Low |
| **Code** | Remove/update control-plane row-policy seeds | Low |
| **Tests** | Update all test fixtures with new domain entities | Medium |
| **Tests** | Update evaluation suite for financial domain | Medium |

**Subsystems Unblocked**: Documentation accuracy, test suite greenness

**Pagila Dependencies Removable**: All remaining references

---

## 4. Testing & Validation Impact

### 4.1 Tests Assuming Pagila

| Test File | Assumption | Remediation |
|-----------|------------|-------------|
| `agent/tests/test_retriever.py` | DB_NAME=pagila in connection | Parameterize |
| `streamlit_app/tests/test_streamlit_agent.py` | Mock data references `films` | Update mocks if domain changes |
| `streamlit_app/tests/test_admin.py` | Example queries about `film`, `actor` | Update fixtures |
| `agent/tests/test_graph.py` | SQL strings reference `film`, `films` | Update fixtures |
| `agent/tests/test_generate_schema.py` | Schema context includes `film`, `actor` | Update fixtures |

### 4.2 Recommendations

| Test Category | Recommendation |
|---------------|----------------|
| Unit tests | **Parameterize** domain-specific values; use fixtures |
| Integration tests | **Dual-support** Pagila and synthetic during transition |
| Evaluation scripts | **Parameterize** DB_NAME; create synthetic golden dataset |
| CI | Run tests against both datasets during transition phase |

---

## 5. Documentation & UX Readiness

### 5.1 Current State vs Claims

| Location | Claim | Reality | Gap |
|----------|-------|---------|-----|
| `README.md` L97 | Diagram shows `PagilaDB` | Pagila is hardcoded everywhere | Update diagram |
| `database/query-target/README.md` L6 | "Dataset agnostic" | True for contract, false for defaults | Accurate but incomplete |
| `.env.example` L43 | `DB_NAME=pagila` | Correct but unclear it's changeable | Document alternatives |

### 5.2 UX Gaps After Cutover

- Quick-start instructions would break if Pagila data missing
- Example queries in docs reference film domain
- No onboarding guide for financial-domain queries

---

## 6. Cutover Checklist

### 6.1 Blocking Items (Must Resolve Before Pagila Removal)

- [ ] **CLI Export**: Add `text2sql-synth export-sql` to emit `01-schema.sql`, `02-data.sql`
- [ ] **Dynamic Policy Enforcement**: Load `ALLOWED_TABLES` from schema introspection, not hardcoded list
- [ ] **Parameterize DB_NAME**: Remove `pagila` default from all code paths (use env var consistently)
- [ ] **Docker Compose Variables**: Use `${DB_NAME:-query_target}` pattern throughout
- [ ] **Financial Domain Patterns**: Create SpaCy patterns for financial terminology (or make patterns fully data-driven)
- [ ] **tables.json Generation**: Add generator to synthetic-data or as separate seeding step
- [ ] **Few-Shot Examples**: Create financial domain query-SQL pairs

### 6.2 Non-Blocking but Recommended

- [ ] Rename container `pagila_db` → `query_target_db`
- [ ] Update README architecture diagram
- [ ] Update `.env.example` default comment
- [ ] Update control-plane row-policies for synthetic schema
- [ ] Create synthetic-data golden dataset for evaluation
- [ ] Update test fixtures to use domain-agnostic patterns

---

## 7. Verdict

### Ready / Not Ready: ❌ NOT READY

**Blocking Issues**:
1. No SQL DDL/DML output from synthetic-data generator
2. Hardcoded `ALLOWED_TABLES` security policy
3. Hardcoded Pagila defaults in 8+ code paths
4. Domain-specific canonicalization cannot parse financial queries

**Estimated Effort to Phase A (Core Runtime)**: 3-5 days  
**Estimated Effort to Phase B (Feature Parity)**: 5-10 days  
**Estimated Effort to Phase C (Full Removal)**: 2-3 days

---

## Appendix: File Reference Quick Lookup

### Pagila Hardcoding Locations

```text
dal/src/dal/database.py:35
mcp-server/src/mcp_server/config/database.py:35
agent/src/agent_core/retriever.py:24
scripts/run_migrations_v2.py:26
agent/scripts/evaluation_metrics.py:17
agent/scripts/run_evaluation.py:25,62,210
docker-compose.infra.yml:8,12,21
docker-compose.app.yml:16,49,95
.env.example:43
```

### Domain-Specific Logic Locations

```text
agent/src/agent_core/validation/policy_enforcer.py:18-34 (ALLOWED_TABLES)
mcp-server/src/mcp_server/services/canonicalization/dependency_patterns.py (all)
mcp-server/src/mcp_server/services/canonicalization/spacy_pipeline.py:208,322-328
mcp-server/src/mcp_server/services/cache/intent_signature.py:92-97,114
mcp-server/src/mcp_server/services/cache/constraint_extractor.py:53,122-123
database/control-plane/06-row-policies.sql:22-30
```
