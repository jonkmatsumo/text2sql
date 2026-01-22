# Scripts

This directory contains shared utility scripts for the repository.

## Structure

- `dev/`: Local development helpers (setup, debug, quick-start).
- `ci/`: Utilities used primarily by CI workflows.
- `docker/`: Docker entrypoints and build helpers.
- `data/`: Data generation, seeding, and management tools.
- `observability/`: OTEL, Grafana, and Metrics helpers.
- `evals/`: Evaluation orchestration and metric calculation.
- `migration/`: One-off database or system migrations.

## Conventions

1. **Repo Root Execution**: All scripts should be runnable from the repository root.
   - Do not assume the current working directory (CWD) is `scripts/` or a service directory.
   - Scripts should calculate the repository root dynamically if needed.

   *Bash:*
   ```bash
   ROOT="$(git rev-parse --show-toplevel)"
   source "$ROOT/scripts/dev/common.sh"
   ```

   *Python:*
   ```python
   from pathlib import Path
   ROOT = Path(__file__).resolve().parents[2] # adjust based on depth
   ```

2. **Environment**: Scripts should check for necessary environment variables (e.g., loaded from `.env`).

3. **naming**: Use `snake_case` for filenames.
