# Gemini Project Context

This document provides context for the Gemini AI agent to understand the project's conventions, architecture, and workflows.

## 1. Project Overview

**Name:** Text 2 SQL MVP
**Goal:** Implement a secure, containerized data access layer using the Model Context Protocol (MCP) to connect an LLM agent to a PostgreSQL database (Pagila dataset).
**Architecture:**
- **Brain:** LLM Agent (LangGraph)
- **Tool:** PostgreSQL Database (Read-Only)
- **Protocol:** MCP (Model Context Protocol) over SSE
- **Frontend:** React Admin UI
- **Observability:** OpenTelemetry + Grafana

## 2. Technology Stack

### Backend
- **Language:** Python 3.12+
- **Package Manager:** `uv` (Workspace mode)
- **Frameworks:**
    - `langgraph` (Agent orchestration)
    - `fastmcp` (MCP Server)
    - `asyncpg` (Database driver)
    - `pydantic` (Data validation)
- **Testing:** `pytest` (Asyncio plugin)

### Frontend (`ui/`)
- **Framework:** React 18
- **Build Tool:** Vite
- **Language:** TypeScript
- **Styling:** (Likely Tailwind or CSS modules - verify if needed)
- **Visualization:** Vega/Vega-Lite

### Infrastructure
- **Containerization:** Docker & Docker Compose
- **Database:** PostgreSQL 16 (Alpine)
- **Orchestration:** Makefiles for common tasks

## 3. Directory Structure

The project follows a monorepo-like structure managed by `uv`.

- **`src/`**: Source code for all packages.
    - `agent/`: LLM Agent logic (LangGraph).
    - `mcp_server/`: MCP Server implementation.
    - `common/`: Shared utilities.
    - `dal/`: Data Access Layer.
- **`pyproject/`**: configurations for each workspace member.
- **`ui/`**: React frontend application.
- **`config/`**: Docker & Service configurations.
- **`tests/`**: Integration and unit tests.
- **`scripts/`**: Utility scripts for data generation, migration, etc.

## 4. Coding Standards

### Python
- **Formatter:** `black` (line-length: 100)
- **Import Sorter:** `isort` (profile: black)
- **Linter:** `flake8`
- **Typing:** Strict type hints required.
- **Docstrings:** Google-style triple quotes.
- **Async:** Use `async/await` for I/O bound operations.

### Frontend (TypeScript)
- **Style:** Follow existing React/TS patterns.
- **Formatting:** Prettier (via pre-commit or IDE).

### Configuration
- **Pre-commit:** Mandatory. Run `pre-commit run --all-files` before PRs.
- **Strictness:** No code that fails pre-commit hooks should be committed.

## 5. Commit Conventions

We follow **Conventional Commits** strictly.

**Format:**
```
type(scope): description

- Detailed bullet point 1
- Detailed bullet point 2
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting, missing semi-colons, etc.
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests or correcting existing tests
- `chore`: Build process, aux tools, etc.

**Examples:**
- `feat(ui-metrics): add metrics preview route`
- `fix(agent): handle null response from LLM`

## 6. Development Workflow

### Setup
1.  **Install `uv`**: `pip install uv` (or brew)
2.  **Install dependencies**: `uv sync`
3.  **Setup Pre-commit**: `pre-commit install`

### Common Commands (Makefile)
- `make app-up`: Start the full application stack.
- `make docker-clean`: Stop and clean containers.
- `make otel-up`: Start observability stack.
- `make help`: Show all commands.

### Testing
- **Backend:** `pytest`
    - `pytest tests/unit`
    - `pytest src/mcp_server/tests`
- **Frontend:** (Check `ui/package.json` scripts)

## 7. Operational Guidelines for AI

1.  **Check Context:** Always read `pyproject.toml` and `Makefile` if unsure about commands.
2.  **Respect Pre-commit:** If you modify files, ensure you didn't break formatting. If you did, run the formatter.
3.  **Follow Directory Structure:** Do not create top-level folders unless necessary. Put code in `src/`.
4.  **Async First:** When writing database or network code, default to `async`.
5.  **Secure:** Never hardcode credentials. Use environment variables.
