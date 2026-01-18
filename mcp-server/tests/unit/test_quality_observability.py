"""Tests for quality observability features."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.pagila
class TestDependencyPatternsWarning:
    """Tests for NLP dependency patterns domain mismatch warning."""

    @pytest.fixture(autouse=True)
    def skip_if_not_pagila(self, dataset_mode):
        """Skip these tests if we are not explicitly running for Pagila."""
        import os

        if os.getenv("RUN_PAGILA_TESTS", "0") == "1":
            return
        if dataset_mode != "pagila":
            pytest.skip("Skipping Pagila tests in synthetic mode")

    def test_warning_when_no_custom_patterns(self, caplog, tmp_path):
        """Test warning is logged when no custom patterns are loaded."""
        import logging

        from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService

        # Create an empty patterns directory (no .jsonl files)
        empty_patterns_dir = tmp_path / "patterns"
        empty_patterns_dir.mkdir()

        # Patch to use our empty patterns directory
        with patch(
            "mcp_server.services.canonicalization.spacy_pipeline.get_env_str",
            return_value=str(empty_patterns_dir),
        ), patch("spacy.load") as mock_spacy_load, patch(
            "spacy.matcher.DependencyMatcher"
        ) as mock_matcher_cls:
            # Setup mock NLP
            mock_nlp = MagicMock()
            mock_nlp.pipe_names = []
            mock_ruler = MagicMock()
            mock_nlp.add_pipe.return_value = mock_ruler
            mock_nlp.vocab = MagicMock()
            mock_spacy_load.return_value = mock_nlp

            mock_matcher = MagicMock()
            mock_matcher_cls.return_value = mock_matcher

            CanonicalizationService.reset_instance()

            # Capture logs from the specific module
            with caplog.at_level(
                logging.WARNING, logger="mcp_server.services.canonicalization.spacy_pipeline"
            ):
                service = CanonicalizationService()
                service._state = None  # Reset state
                # Build pipeline with no custom patterns
                service._build_pipeline("en_core_web_sm", extra_patterns=None)

            assert "nlp_dependency_patterns_default_only" in caplog.text
            assert "domain_assumption=film_schema" in caplog.text

    def test_no_warning_when_custom_patterns_loaded(self, caplog):
        """Test no warning when custom patterns are provided."""
        from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService

        with patch("spacy.load") as mock_spacy_load, patch(
            "spacy.matcher.DependencyMatcher"
        ) as mock_matcher_cls:
            mock_nlp = MagicMock()
            mock_nlp.pipe_names = []
            mock_nlp.add_pipe.return_value = MagicMock()
            mock_nlp.vocab = MagicMock()
            mock_spacy_load.return_value = mock_nlp

            mock_matcher = MagicMock()
            mock_matcher_cls.return_value = mock_matcher

            CanonicalizationService.reset_instance()

            with caplog.at_level("WARNING"):
                service = CanonicalizationService()
                # Build pipeline WITH custom patterns
                service._build_pipeline(
                    "en_core_web_sm",
                    extra_patterns=[{"label": "CUSTOM", "pattern": "test"}],
                )

            assert "nlp_dependency_patterns_default_only" not in caplog.text


class TestGoldenDatasetIsolation:
    """Test that runtime modules don't import golden_dataset references."""

    def test_no_golden_dataset_in_runtime_imports(self):
        """Ensure runtime modules don't reference golden_dataset."""
        from pathlib import Path

        # List of runtime modules that should NOT reference golden_dataset
        runtime_modules = [
            "mcp_server.main",
            "mcp_server.services.registry.service",
            "mcp_server.services.seeding.cli",
        ]

        # Resolve repo root relative to this test file
        # tests/unit/test_quality_observability.py -> ../.. -> mcp-server root
        mcp_server_root = Path(__file__).parent.parent.parent
        src_root = mcp_server_root / "src"

        for module_path in runtime_modules:
            # Convert module path to file path
            relative_path = module_path.replace(".", "/") + ".py"
            full_path = src_root / relative_path

            # Ensure file exists so test remains valid
            assert full_path.exists(), f"Runtime module {module_path} not found at {full_path}"

            # Read content natively (no subprocess/grep)
            content = full_path.read_text("utf-8")

            assert "golden_dataset" not in content, (
                f"Runtime module {module_path} references golden_dataset! "
                "golden_dataset should only be used in evaluation scripts."
            )


class TestRegistryStatusLogging:
    """Tests for few-shot registry status logging."""

    @pytest.mark.asyncio
    async def test_registry_status_logged(self, caplog):
        """Test registry status is logged with correct count."""
        from mcp_server.services.registry import RegistryService

        # Mock list_examples to return some examples
        mock_examples = [MagicMock(), MagicMock(), MagicMock()]

        with patch.object(RegistryService, "list_examples", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = mock_examples

            examples = await RegistryService.list_examples(tenant_id=1, limit=1000)

            assert len(examples) == 3

    @pytest.mark.asyncio
    async def test_registry_empty_returns_empty_list(self, caplog):
        """Test registry returns empty list when no examples."""
        from mcp_server.services.registry import RegistryService

        with patch.object(RegistryService, "list_examples", new_callable=AsyncMock) as mock_list:
            mock_list.return_value = []

            examples = await RegistryService.list_examples(tenant_id=1, limit=1000)

            assert len(examples) == 0
