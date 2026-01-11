import os


class PipelineConfig:
    """Configuration loader for the semantic enrichment pipeline."""

    def __init__(self, dry_run: bool = False):
        """Initialize pipeline configuration with strict environment validation."""
        self.dry_run = dry_run
        self.enable_llm_enrichment = os.getenv("ENABLE_LLM_ENRICHMENT", "").lower() == "true"

        if not self.enable_llm_enrichment and not self.dry_run:
            raise RuntimeError(
                "Enrichment pipeline requires ENABLE_LLM_ENRICHMENT='true' environment variable."
            )
