# Text 2 SQL

A secure, containerized Text 2 SQL agent that enables natural language querying of enterprise data warehouses through the Model Context Protocol (MCP). Phase 1 and Phase 2 (RAG Integration) are complete.

## 🎯 Overview

This project implements Phase 1 of a Text 2 SQL system, focusing on establishing a secure data access layer that decouples the AI reasoning engine from the underlying database infrastructure. The system uses Docker Compose to orchestrate:

- **PostgreSQL 16** with pgvector extension and the Pagila dataset (DVD rental business schema)
- **Python MCP Server** (fastmcp) providing secure, read-only database access tools
- **RAG Engine** with semantic schema retrieval using vector embeddings

## 🏗️ Architecture

The architecture follows the "Agentic" pattern where the AI agent (Brain) communicates with the data warehouse (Tool) through a standardized MCP interface. This decoupling ensures:

- **Vendor Agnostic**: Works with PostgreSQL, Snowflake, Databricks, etc.
- **Secure**: Multi-layered security (container isolation, least privilege, application gates)
- **RAG-Enhanced**: Semantic schema retrieval solves context window bottleneck (Phase 2 complete)

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

2. **Download database initialization files**
   ```bash
   # The data file (02-data.sql) is gitignored due to size (~3.2MB)
   # Run the download script to fetch it along with the schema
   ./database/init-scripts/download_data.sh
   ```
   **Note**: The `02-data.sql` file is not committed to git (it's in `.gitignore`) because it's a large generated file (~3.2MB). The download script will fetch both the schema and data files from the official Pagila repository.

3. **Create environment file** (optional)
   ```bash
   # .env file is optional - docker-compose.yml has defaults
   # Create it only if you want to override default credentials
   ```

4. **Start the services**
   ```bash
   docker compose up --build
   ```

5. **Verify the setup**
   - Database: Connect to `localhost:5432` (user: `postgres`, password: from `.env` or default `root_password`)
   - MCP Server: Access SSE endpoint at `http://localhost:8000/sse`

6. **Test with MCP Inspector**
   ```bash
   npx @modelcontextprotocol/inspector
   # Connect to: http://localhost:8000/sse
   ```

## 📋 MCP Tools

The MCP server exposes five tools:

1. **`list_tables`** ✅: Discover available tables (with optional fuzzy search) - *Implemented*
2. **`get_table_schema`** ✅: Retrieve detailed schema metadata (columns, types, foreign keys) - *Implemented*
3. **`execute_sql_query`** ✅: Execute read-only SQL queries (with safety checks) - *Implemented*
4. **`get_semantic_definitions`** ✅: Retrieve business metric definitions - *Implemented*
5. **`search_relevant_tables`** ✅: Semantic search for relevant tables using RAG - *Implemented* (Phase 2)

All tools are implemented and the MCP server is ready for use.

### RAG-Enhanced Schema Retrieval (Phase 2)

The `search_relevant_tables` tool uses Retrieval-Augmented Generation (RAG) to solve the context window bottleneck:
- **Semantic Search**: Uses vector embeddings to find tables relevant to natural language queries
- **Automatic Indexing**: Schema embeddings are automatically created on server startup
- **Enhanced Metadata**: Schema documents include semantic hints (identifiers, timestamps, monetary values)
- **Similarity Scoring**: Returns tables ranked by semantic similarity to the query

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
├── pytest.ini                  # Pytest configuration (root level)
├── .github/
│   ├── workflows/              # GitHub Actions CI/CD
│   │   ├── ci.yml             # Continuous Integration
│   │   └── test.yml           # Test suite
│   └── dependabot.yml         # Dependency updates
├── database/
│   └── init-scripts/           # Database initialization
│       ├── download_data.sh    # Script to download Pagila files
│       ├── 01-schema.sql       # Pagila schema (committed)
│       ├── 02-data.sql         # Pagila data (gitignored, ~3.2MB)
│       ├── 03-permissions.sql  # Security configuration
│       └── 04-vector-setup.sql # pgvector extension and schema_embeddings table
└── mcp-server/
    ├── Dockerfile
    ├── pyproject.toml          # Python dependencies
    ├── pytest.ini              # Pytest configuration (module level)
    ├── src/
    │   ├── main.py             # MCP server entrypoint
    │   ├── db.py               # Database connection pool
    │   ├── tools.py            # MCP tool implementations
    │   ├── rag.py              # RAG engine (embeddings, vector search)
    │   └── indexer.py          # Schema indexing service
    ├── scripts/
    │   └── test_rag.py         # End-to-end RAG verification
    └── tests/
        ├── __init__.py
        ├── test_db.py          # Database module unit tests
        ├── test_tools.py       # Tools module unit tests
        ├── test_rag.py         # RAG engine unit tests
        ├── test_indexer.py     # Indexer service unit tests
        └── test_main.py        # Main entrypoint tests
```

## 🧪 Testing

### Unit Tests

The project includes comprehensive unit tests using pytest with mocking (no database required).

**Run all tests:**
```bash
# From project root (recommended - runs both agent and mcp-server tests)
# The pytest.ini config automatically uses --import-mode=importlib
pytest

# Or explicitly specify import mode
pytest --import-mode=importlib

# Or from individual directories
cd mcp-server
pytest tests/ -v

cd agent
pytest tests/ -v
```

**Note**: When running tests from the root directory, pytest uses `--import-mode=importlib` (configured in `pytest.ini`) to avoid module conflicts when collecting tests from both `agent/tests` and `mcp-server/tests`. This is required because both directories have a `tests` package.

**Run with coverage:**
```bash
# From root (covers both agent and mcp-server)
pytest --cov=mcp-server/src --cov=agent/src --cov-report=term-missing

# Or for individual modules
pytest mcp-server/tests/ --cov=mcp-server/src --cov-report=term-missing
```

**Run specific test file:**
```bash
pytest mcp-server/tests/test_db.py -v      # Database module tests
pytest mcp-server/tests/test_tools.py -v   # Tools module tests
pytest agent/tests/test_state.py -v        # Agent state tests
```

**Test Coverage:**
- `db.py`: 100% coverage (8 tests)
- `tools.py`: 100% coverage (51 tests covering all 5 tools)
- `rag.py`: 100% coverage (20 tests)
- `indexer.py`: 100% coverage (6 tests)
- `main.py`: 100% coverage (9 tests)

**Total**: 100+ unit tests with comprehensive coverage

**Install test dependencies:**
```bash
cd mcp-server
pip install -e ".[test]"
```

### Integration Tests

**Database Health Check:**
```bash
docker ps  # Both containers should be "Up (healthy)"
psql -h localhost -U postgres -d pagila  # Verify schema and data
```

**MCP Server Test:**
```bash
curl -v http://localhost:8000/sse  # Should return 200 OK with SSE headers
```

**Security Verification:**
Attempt to execute a mutative query via the MCP tools - it should be rejected at the application layer.

### Code Quality

This project uses pre-commit hooks to enforce code quality. The configuration is defined in `.pre-commit-config.yaml`.

**Setup:**

**Option 1: Use the setup script (recommended)**
```bash
./setup-pre-commit.sh
```

**Option 2: Manual setup**
```bash
# Install pre-commit (use pip3 on macOS if pip is not available)
pip3 install pre-commit
# or if pip3 doesn't work:
python3 -m pip install pre-commit

# Install git hooks (this makes hooks run on 'git commit')
pre-commit install
# or if pre-commit command not found:
python3 -m pre_commit install

# Run hooks manually on all files (optional, to fix existing files)
pre-commit run --all-files
# or if pre-commit command not found:
python3 -m pre_commit run --all-files
```

**Running Pre-commit Hooks:**
- **Important**: Pre-commit may not be in PATH. Use: `python3 -m pre_commit run --all-files`
- If pre-commit is not installed, install it first: `python3 -m pip install pre-commit`
- Pre-commit hooks will auto-format files (black, isort). Re-run to verify all hooks pass after auto-fixes
- Always run pre-commit hooks after making changes to source code, before running tests

**Troubleshooting**: If hooks still don't run, see `TROUBLESHOOTING.md` for detailed solutions.

**Note**: On macOS, you may need to use `pip3` instead of `pip`, or use `python3 -m pre_commit` if the `pre-commit` command is not in your PATH.

**Hooks configured:**
- **File checks**: Trailing whitespace, end-of-file fixer, YAML/JSON/TOML validation, large file detection, merge conflict detection
- **Python formatting**: Black (100 char line length) with Python 3.12
- **Python linting**: flake8 (100 char line length, ignores E203, W503, D100)
- **Import sorting**: isort (Black profile, 100 char line length)
- **YAML formatting**: Prettier (excludes docker-compose.yml)
- **Dockerfile linting**: Handled in CI/CD pipeline (not in pre-commit hooks)

**Important**: Some hooks (trailing-whitespace, end-of-file-fixer, prettier) automatically fix files. If a hook modifies files during commit:
1. The commit will be blocked
2. Stage the auto-fixed files: `git add .`
3. Commit again: `git commit -m "your message"`

This ensures all code is properly formatted before it's committed.

### Development Workflow

**ALWAYS run pre-commit hooks AND tests after making changes to source code**

**Standard Workflow:**
1. Make code changes
2. Run pre-commit hooks: `python3 -m pre_commit run --all-files`
3. Run tests: `./venv/bin/pytest mcp-server/tests/ -v` (or specific test file/class)
4. Verify all hooks and tests pass
5. If pre-commit auto-fixes files, re-run it to verify all hooks pass
6. Never commit code that fails pre-commit hooks or tests

**Quick Commands:**
```bash
# Run pre-commit hooks
python3 -m pre_commit run --all-files

# Run all tests
./venv/bin/pytest mcp-server/tests/ -v

# Run specific test file
./venv/bin/pytest mcp-server/tests/test_tools.py -v

# Run tests with coverage
./venv/bin/pytest mcp-server/tests/ --cov=src --cov-report=term-missing
```

### CI/CD

GitHub Actions workflows are configured in `.github/workflows/`. The CI pipeline runs on every push/PR to `main` and `develop` branches.

**CI Workflow** (`.github/workflows/ci.yml`):
- **Lint job**: Runs pre-commit hooks on all files to ensure code quality
- **Docker Build job**: Builds and validates the MCP server Docker image, validates docker-compose configuration
- **Database Init job**: Tests database initialization scripts (validates SQL file structure and syntax)
- **Security Scan job**: Runs Trivy vulnerability scanner and uploads results to GitHub Security

**Test Workflow** (`.github/workflows/test.yml`):
- Runs unit tests with pytest
- Includes PostgreSQL service setup for integration testing
- Generates coverage reports

**Dependabot** (`.github/dependabot.yml`):
- Automatically creates PRs for dependency updates:
  - Python packages in `mcp-server/` (weekly)
  - Docker images (weekly)
  - GitHub Actions (weekly)
