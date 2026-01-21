"""Tests for RagEngine."""

import os
from unittest.mock import patch

import pytest

from mcp_server.services.rag.engine import RagEngine


@pytest.fixture
def mock_provider_env():
    """Fixture to set RAG_EMBEDDING_PROVIDER=mock."""
    with patch.dict(os.environ, {"RAG_EMBEDDING_PROVIDER": "mock"}):
        # Reset model to force reload
        RagEngine._model = None
        yield
        RagEngine._model = None


@pytest.mark.asyncio
async def test_embed_text_mock(mock_provider_env):
    """Test embedding single text with mock provider."""
    text = "Hello world"
    vector = await RagEngine.embed_text(text)

    assert isinstance(vector, list)
    assert len(vector) == 384
    assert all(isinstance(x, float) for x in vector)


@pytest.mark.asyncio
async def test_embed_batch_mock(mock_provider_env):
    """Test embedding batch texts with mock provider."""
    texts = ["Hello", "World"]
    vectors = await RagEngine.embed_batch(texts)

    assert isinstance(vectors, list)
    assert len(vectors) == 2
    for v in vectors:
        assert len(v) == 384


@pytest.mark.asyncio
async def test_mock_determinism(mock_provider_env):
    """Test that mock embeddings are deterministic."""
    text1 = "Same Text"
    text2 = "Same Text"
    text3 = "Different Text"

    v1 = await RagEngine.embed_text(text1)
    v2 = await RagEngine.embed_text(text2)
    v3 = await RagEngine.embed_text(text3)

    assert v1 == v2
    assert v1 != v3
