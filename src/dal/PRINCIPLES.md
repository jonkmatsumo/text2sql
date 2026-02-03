# DAL Principles

This DAL is intentionally conservative and **must not hide or normalize database semantics**.

## Guardrails
- No semantic hiding: provider-specific behavior stays explicit.
- Capability-gated features only: new behavior must be driven by provider capabilities.
- Experimental behavior is opt-in and disabled by default.
- Each change must be reversible without impacting existing providers.

## Experimental Features
Set `DAL_EXPERIMENTAL_FEATURES=true` to enable experimental, capability-gated features.
