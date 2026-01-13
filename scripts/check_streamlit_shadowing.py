#!/usr/bin/env python3
import os
import sys

import streamlit


def check_shadowing():
    """Check if the imported 'streamlit' package is shadowing the real PyPI package."""
    sl_path = os.path.abspath(streamlit.__file__)
    print(f"Streamlit resolved path: {sl_path}")

    # Heuristic: Fail if path contains 'streamlit/__init__.py'
    # AND does NOT contain 'site-packages'/'dist-packages'
    # This catches the case where the local directory is picked up.

    is_site_packages = "site-packages" in sl_path or "dist-packages" in sl_path
    is_local_repo = "streamlit/__init__.py" in sl_path

    if is_local_repo and not is_site_packages:
        print("\n[ERROR] Shadowing detected!")
        print(
            "Your environment is importing the repo's 'streamlit/' "
            "package instead of the PyPI 'streamlit' library."
        )
        print(f"Resolved path: {sl_path}")
        print("\nRemediation:")
        print("1. Ensure you are running from the repository root.")
        print("2. Verify that 'streamlit/' is not in your PYTHONPATH.")
        print("3. (Future) We will remove 'streamlit/__init__.py' in Phase 2.")
        sys.exit(1)

    print("[OK] No shadowing detected. Using installed streamlit package.")
    sys.exit(0)


if __name__ == "__main__":
    check_shadowing()
