# Streamlit Directory Layout

This directory (`streamlit/`) serves as the **packaging boundary** and **Docker build context** for the UI service. It contains configuration, tests, and entrypoints.

## Where is the source code?

The actual application source code is located in **`streamlit_app/`** at the repository root.

**Why the separation?**
We use `streamlit_app/` for the source code to avoid package name conflicts with the external PyPI `streamlit` library. This ensures that `import streamlit` correctly resolves to the library, while internal modules are imported via `streamlit_app.*`.
