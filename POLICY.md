# Security Policy & Boundary Definition

This document defines the separation of duties between the **Agent** (Policy Layer) and the **Execution Server** (Safety Sandbox).

## 1. Architecture Overview

```mermaid
graph TD
    User[User Question] --> Agent[Agent (Python)]
    Agent -->|1. Generate SQL| LLM
    Agent -->|2. Enforce Policy| PolicyEnforcer
    PolicyEnforcer -->|3. Rewrite for Tenant| TenantRewriter
    TenantRewriter -->|4. Execute Tool| MCPServer[MCP Server (Python)]
    MCPServer -->|5. Verify Safety| ASTValidator
    ASTValidator -->|6. Run| Database
```

## 2. Agent Layer: Policy & Business Logic
**Responsibility:** Enforce *who* can access *what*.
**Location:** `src/agent/validation/policy_enforcer.py` & `tenant_rewriter.py`

| Check | Description | Implementation |
| :--- | :--- | :--- |
| **Statement Whitelist** | Only allow data retrieval (SELECT, UNION, INTERSECT, EXCEPT). | `PolicyEnforcer.validate_sql` (AST) |
| **Table Allowlist** | Restrict access to `public` schema tables only. | `PolicyEnforcer.validate_sql` (Introspection) |
| **Cross-Schema Block** | Prevent access to system schemas (e.g. `information_schema`, `pg_catalog`). | `PolicyEnforcer.validate_sql` |
| **Function Blocklist** | Block dangerous functions (e.g. `pg_read_file`, `system`). | `PolicyEnforcer` Blocklist |
| **Tenant Isolation** | Inject mandatory `WHERE tenant_id = :id` predicates. | `TenantRewriter` (AST Transformation) |

## 3. Server Layer: Safety Sandbox
**Responsibility:** Enforce *execution safety* and *resource limits*.
**Location:** `src/mcp_server/tools/execute_sql_query.py`

| Check | Description | Implementation |
| :--- | :--- | :--- |
| **AST Sanity** | Ensure SQL is a valid, single statement. | `_validate_sql_ast` (sqlglot) |
| **Read-Only Enforcement** | Ensure connection is read-only (DB level) and AST is SELECT-only. | `_validate_sql_ast` + `read_only=True` DB conn |
| **Resource Limits** | Max rows, max bytes, query timeout. | `fetch_page`, `JSONBudget`, `run_with_timeout` |
| **Capability Negotiation** | Handle client capabilities (e.g. pagination support). | `capability_negotiation.py` |

## 4. Boundary Logic

1.  **Defense in Depth**: The Server does *not* trust the Agent. It re-validates that the query is a SELECT statement using its own AST parser.
2.  **DB Permissions**: The final backstop is the database user permissions (Text2SQL user should be `limited_read_only` with NO access to sensitive system tables).
3.  **No Mutation**: Neither layer allows `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`.

## 5. Protocol
- The Agent sends: `sql`, `tenant_id` (header/arg).
- The Server executes: `set_config('app.current_tenant', tenant_id)` (if RLS enabled) or relies on rewrote SQL.
- *Current Implementation*: The Agent rewrites SQL to include `tenant_id` predicates directly. The Server validates the *syntax* but assumes the *filtering* is correct (trusts the Agent's intent, but validates the safety).
