#!/usr/bin/env python3
import os
import re
import sys


def validate_dockerfile(dockerfile_path):
    """Validate that all COPY source paths in a Dockerfile exist."""
    if not os.path.exists(dockerfile_path):
        print(f"Error: {dockerfile_path} not found.")
        return False

    print(f"Validating {dockerfile_path}...")
    all_exist = True

    with open(dockerfile_path, "r") as f:
        for line in f:
            # Match COPY src dest (handles basic cases)
            match = re.match(r"^COPY\s+([^\s]+)\s+([^\s]+)", line.strip())
            if match:
                src = match.group(1)
                # Remove trailing slash for directory checks
                src_path = src.rstrip("/")
                if not os.path.exists(src_path):
                    print(f"  [FAIL] Path does not exist: {src}")
                    all_exist = False
                else:
                    print(f"  [OK] Found: {src}")

    return all_exist


if __name__ == "__main__":
    # Check current directory context
    success = validate_dockerfile("mcp-server/Dockerfile")
    if not success:
        sys.exit(1)
    print("Docker context validation successful.")
