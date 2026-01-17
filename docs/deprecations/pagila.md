# Pagila Dataset Deprecation Notice

**Status:** Deprecated  
**Effective Date:** 2026-01-17  
**Replacement:** Synthetic Financial Dataset (`DATASET_MODE=synthetic`)

## Overview

The Pagila dataset (a port of the DVD rental database) is officially deprecated as the query target for the Text-to-SQL agent. It has been replaced by a purpose-built synthetic financial dataset that better reflects the target domain and requirements of the system.

## Reason for Deprecation

1.  **Domain Mismatch:** Pagila models a DVD rental store, which does not align with the financial/banking domain of the Text-to-SQL agent.
2.  **Schema Rigidity:** Pagila's schema is fixed and difficult to evolve for testing complex SQL patterns (e.g., specific join types, time-series analysis).
3.  **Data Control:** The synthetic dataset allows for precise control over data distribution, edge cases, and golden answer generation, which is critical for robust evaluation.

## Migration Guide

The system now defaults to the synthetic dataset. No action is required for new installations.

To ensure you are using the supported dataset:

1.  Check your `.env` file or environment variables.
2.  Ensure `DATASET_MODE` is set to `synthetic` (or unset, as it defaults to `synthetic`).
3.  If you have explicit `DB_NAME=pagila` configuration, remove it or update it to match the synthetic database name.

### Legacy Usage

Support for Pagila is maintained in "legacy mode" for backward compatibility verification only. To force the use of Pagila:

```bash
export DATASET_MODE=pagila
```

**Warning:** Running in Pagila mode will emit a runtime warning and may not support recent features or query patterns.

## Removal Timeline

-   **Phase 1 (Current):** Pagila is deprecated but available via explicit configuration. Runtime warnings are emitted.
-   **Phase 2 (Future):** Pagila support will be removed from the codebase. Related tests and artifacts will be deleted.
