import os
from unittest.mock import patch

import pytest
from mcp_server.graph_ingestion.enrichment.config import PipelineConfig
from mcp_server.graph_ingestion.enrichment.hashing import generate_canonical_hash


class TestPipelineConfig:
    """Test suite for PipelineConfig."""

    def test_init_raises_error_without_env_var(self):
        """Test that PipelineConfig raises RuntimeError when env var is missing and not dry_run."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                RuntimeError, match="Enrichment pipeline requires ENABLE_LLM_ENRICHMENT='true'"
            ):
                PipelineConfig()

    def test_init_allows_missing_env_var_in_dry_run(self):
        """Test that PipelineConfig allows missing env var when dry_run is True."""
        with patch.dict(os.environ, {}, clear=True):
            config = PipelineConfig(dry_run=True)
            assert config.dry_run is True
            assert config.enable_llm_enrichment is False

    def test_init_success_with_env_var(self):
        """Test that PipelineConfig initializes correctly when env var is set."""
        with patch.dict(os.environ, {"ENABLE_LLM_ENRICHMENT": "true"}, clear=True):
            config = PipelineConfig()
            assert config.enable_llm_enrichment is True


class TestHashing:
    """Test suite for hashing utilities."""

    def test_generate_canonical_hash_determinism(self):
        """Test that dictionary key order does not affect the hash."""
        data1 = {"a": 1, "b": 2}
        data2 = {"b": 2, "a": 1}

        hash1 = generate_canonical_hash(data1)
        hash2 = generate_canonical_hash(data2)

        assert hash1 == hash2

    def test_generate_canonical_hash_nested(self):
        """Test that nested dictionary key order does not affect the hash."""
        data1 = {"root": {"x": 10, "y": 20}}
        data2 = {"root": {"y": 20, "x": 10}}

        hash1 = generate_canonical_hash(data1)
        hash2 = generate_canonical_hash(data2)

        assert hash1 == hash2
