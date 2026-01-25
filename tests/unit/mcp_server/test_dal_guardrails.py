import os
from pathlib import Path

import pytest

# These tests are guardrails to prevent regression of the DAL migration.
# They ensure that:
# 1. No code reappears in src/mcp_server/dal
# 2. No imports from mcp_server.dal are introduced anywhere in the repo


def test_no_dal_code_in_mcp():
    """Ensure the directory src/mcp/dal does not exist."""
    # Assuming the test is running from the repo root or we can locate the root
    # Adjust path finding logic to be robust
    repo_root = Path(__file__).resolve().parents[3]
    dal_path = repo_root / "src" / "mcp_server" / "dal"

    # We intend for this directory to be gone entirely.
    # If it exists, it must be empty or only contain __pycache__ (though ideally gone).
    # Strictest check: assert it does not exist.
    assert not dal_path.exists(), (
        f"Vestigial directory {dal_path} exists. It should have been removed "
        "to prevent confusion with the real DAL."
    )


def test_no_mcp_dal_imports_repo_wide():
    """Scan the entire repository for any Python files importing from 'mcp_server.dal'."""
    repo_root = Path(__file__).resolve().parents[3]

    forbidden_patterns = ["from mcp_server.dal", "import mcp_server.dal", "mcp_server.dal."]

    # Directories to exclude from the scan
    excludes = {
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        "dist",
        "build",
        ".mypy_cache",
        ".ruff_cache",
        "node_modules",
        ".gemini",  # exclude agent artifacts/memory
    }

    offending_files = []

    for root, dirs, files in os.walk(repo_root):
        # Modify dirs in-place to skip ignored directories
        dirs[:] = [d for d in dirs if d not in excludes]

        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file

                # Skip this very file to avoid matching the test strings themselves
                if file_path.resolve() == Path(__file__).resolve():
                    continue

                # Skip other test files that legitimately check for these patterns
                if file == "test_architecture_boundaries.py":
                    continue

                try:
                    content = file_path.read_text(errors="ignore")
                    for i, line in enumerate(content.splitlines()):
                        if any(pattern in line for pattern in forbidden_patterns):
                            rel_path = file_path.relative_to(repo_root)
                            msg = f"{rel_path}:{i+1}: {line.strip()}"
                            offending_files.append(msg)
                            break  # Stop scanning this file after first match
                except Exception:
                    # Fail securely if we can't read a file? Or just log?
                    # For a guardrail, probably safe to ignore unreadable files
                    # if they aren't source code
                    pass

    if offending_files:
        pytest.fail(
            "Found forbidden imports of 'mcp_server.dal'. "
            "Please use the top-level 'dal' package instead.\n" + "\n".join(offending_files)
        )
