# Text 2 SQL - Phase 1 MVP

A secure, containerized Text 2 SQL agent that enables natural language querying of enterprise data warehouses through the Model Context Protocol (MCP).

## 🎯 Overview

This project implements Phase 1 of a Text 2 SQL system, focusing on establishing a secure data access layer that decouples the AI reasoning engine from the underlying database infrastructure. The system uses Docker Compose to orchestrate:

- **PostgreSQL 16** with the Pagila dataset (DVD rental business schema)
- **Python MCP Server** (fastmcp) providing secure, read-only database access tools

## 🏗️ Architecture

The architecture follows the "Agentic" pattern where the AI agent (Brain) communicates with the data warehouse (Tool) through a standardized MCP interface. This decoupling ensures:

- **Vendor Agnostic**: Works with PostgreSQL, Snowflake, Databricks, etc.
- **Secure**: Multi-layered security (container isolation, least privilege, application gates)
- **Scalable**: Modular design ready for Phase 2 (RAG integration)

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Python 3.12+ (for local development)
- Git installed and configured
- MCP client (e.g., Claude Desktop, or `@modelcontextprotocol/inspector`)

### Setup

1. **Clone and navigate to the project**
   ```bash
   cd text2sql
   ```

2. **Create environment file**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Start the services**
   ```bash
   docker compose up --build
   ```

4. **Verify the setup**
   - Database: Connect to `localhost:5432` (user: `postgres`, password: from `.env`)
   - MCP Server: Access SSE endpoint at `http://localhost:8000/sse`

5. **Test with MCP Inspector**
   ```bash
   npx @modelcontextprotocol/inspector
   # Connect to: http://localhost:8000/sse
   ```

## 📋 MCP Tools

The MCP server exposes four tools:

1. **`list_tables`**: Discover available tables (with optional fuzzy search)
2. **`get_table_schema`**: Retrieve detailed schema metadata (columns, types, foreign keys)
3. **`execute_sql_query`**: Execute read-only SQL queries (with safety checks)
4. **`get_semantic_definitions`**: Retrieve business metric definitions

## 🔒 Security

- **Read-Only Access**: Database user `bi_agent_ro` has SELECT privileges only
- **Application Gates**: Regex-based pre-flight checks reject mutative SQL keywords
- **Container Isolation**: Services communicate via Docker bridge network
- **Error Sanitization**: Error messages exclude internal paths and credentials

## 📁 Project Structure

```
text2sql/
├── docker-compose.yml          # Service orchestration
├── .env                        # Environment variables (not in git)
├── .pre-commit-config.yaml     # Pre-commit hooks configuration
├── .github/
│   ├── workflows/              # GitHub Actions CI/CD
│   │   ├── ci.yml             # Continuous Integration
│   │   └── test.yml           # Test suite
│   └── dependabot.yml         # Dependency updates
├── database/
│   └── init-scripts/           # Database initialization
│       ├── 01-schema.sql       # Pagila schema
│       ├── 02-data.sql         # Pagila data
│       └── 03-permissions.sql  # Security configuration
└── mcp-server/
    ├── Dockerfile
    ├── pyproject.toml          # Python dependencies
    └── src/
        ├── main.py             # MCP server entrypoint
        ├── db.py               # Database connection pool
        └── tools.py            # MCP tool implementations
```

## 🧪 Testing

### Database Health Check
```bash
docker ps  # Both containers should be "Up (healthy)"
psql -h localhost -U postgres -d pagila  # Verify schema and data
```

### MCP Server Test
```bash
curl -v http://localhost:8000/sse  # Should return 200 OK with SSE headers
```

### Security Verification
Attempt to execute a mutative query via the MCP tools - it should be rejected at the application layer.

## 📚 Documentation

Detailed implementation guides and technical specifications are available in the `docs/` directory (local only, not committed to git).

## 🔄 Development Workflow

1. **Sprint 1**: Infrastructure & Database Initialization (includes pre-commit hooks & GitHub Actions)
2. **Sprint 2**: MCP Server Development
3. **Sprint 3**: Integration & Verification

See `docs/implementation-guide.md` for detailed, step-by-step instructions optimized for Cursor workflow.

### Code Quality

This project uses pre-commit hooks to enforce code quality. The configuration is defined in `.pre-commit-config.yaml`.

**Setup:**
```bash
# Install pre-commit hooks
pip install pre-commit
pre-commit install

# Run hooks manually on all files
pre-commit run --all-files
```

**Hooks configured:**
- **File checks**: Trailing whitespace, end-of-file fixer, YAML/JSON/TOML validation, large file detection, merge conflict detection
- **Python formatting**: Black (100 char line length) with Python 3.12
- **Python linting**: flake8 (100 char line length, ignores E203, W503)
- **Import sorting**: isort (Black profile, 100 char line length)
- **YAML formatting**: Prettier (excludes docker-compose.yml)
- **Dockerfile linting**: hadolint (ignores DL3008, DL3009)

### CI/CD

GitHub Actions workflows are configured in `.github/workflows/`. The CI pipeline runs on every push/PR to `main` and `develop` branches.

**CI Workflow** (`.github/workflows/ci.yml`):
- **Lint job**: Runs pre-commit hooks on all files to ensure code quality
- **Docker Build job**: Builds and validates the MCP server Docker image, validates docker-compose configuration
- **Database Init job**: Tests database initialization scripts (validates SQL file structure and syntax)
- **Security Scan job**: Runs Trivy vulnerability scanner and uploads results to GitHub Security

**Test Workflow** (`.github/workflows/test.yml`):
- Placeholder for future MCP server integration tests
- Includes PostgreSQL service setup for testing

**Dependabot** (`.github/dependabot.yml`):
- Automatically creates PRs for dependency updates:
  - Python packages in `mcp-server/` (weekly)
  - Docker images (weekly)
  - GitHub Actions (weekly)

## 🤝 Contributing

This is a Phase 1 MVP. Future phases will include:
- Phase 2: RAG Integration for enhanced context awareness
- Phase 3: Multi-tenant support with Row-Level Security
- Phase 4: Advanced query optimization and caching

## 📄 License

[Specify your license here]

