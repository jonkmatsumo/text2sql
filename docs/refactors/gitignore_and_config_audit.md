# Gitignore & Config Audit

## 1. Ignore Candidates
Directories found that should be ignored:
- `local-data/`
- `dummy_out/`
- `o/` (contains run outputs)
- `out/` (standard output dir)
- `outputs/` (standard output dir)

## 2. Tracked Files in Ignored Paths
- None found (git ls-files returned no matches).

## 3. Config Files (Inventory)

### Tooling Candidates (for `config/tooling/`)
- `pytest.ini` (Root)
- `.pre-commit-config.yaml` (Root - likely keep at root)
- `docker-compose*.yml` (Root - keep at root or config/docker)

### Service Configs (for `config/services/<service>/`)
- `observability/otel-collector-config.yaml`
- `observability/tempo.yaml`
- `observability/otel-worker/alembic.ini`
- `streamlit-app/.streamlit/config.toml`
- `src/text2sql_synth/scripts/catalog.yaml`

### Packaging Manifests (Keep in `pyproject/`)
- `pyproject/agent/pyproject.toml`
- `pyproject/common/pyproject.toml`
- `pyproject/dal/pyproject.toml`
- `pyproject/ingestion/pyproject.toml`
- `pyproject/mcp-server/pyproject.toml`
- `pyproject/otel-worker/pyproject.toml`
- `pyproject/schema/pyproject.toml`
- `pyproject/streamlit-app/pyproject.toml`
- `pyproject/synthetic-data/pyproject.toml`

## 4. Empty Directories
- `scripts/_archive`
- `scripts/ci`
- `scripts/docker`
- `tests/e2e`

## 5. Empty Subpackages (Candidates for removal)
- `src/ingestion/graph`
- `src/ingestion/cli`
- `src/mcp_server/rag`
- `src/mcp_server/models/database`
- `src/mcp_server/models/graph`
- `src/mcp_server/models/rag`
