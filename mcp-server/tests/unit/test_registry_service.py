"""Unit tests for the RegistryService."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.models import QueryPair
from mcp_server.services.registry import RegistryService


@pytest.fixture
def mock_canonicalizer():
    """Mock the CanonicalizationService instance."""
    with patch("mcp_server.services.canonicalization.CanonicalizationService.get_instance") as mock:
        instance = mock.return_value
        instance.process_query = AsyncMock(return_value=({}, "FINGERPRINT", "SIG_KEY"))
        yield instance


@pytest.fixture
def mock_rag_engine():
    """Mock the RagEngine embedding function."""
    with patch("mcp_server.services.rag.RagEngine.embed_text") as mock:
        mock.return_value = [0.1] * 1536  # Standard embedding size
        yield mock


@pytest.fixture
def mock_store():
    """Mock the RegistryStore backend."""
    with patch("mcp_server.services.registry.service.get_registry_store") as mock:
        store = AsyncMock()
        mock.return_value = store
        yield store


@pytest.mark.asyncio
async def test_register_pair(mock_canonicalizer, mock_rag_engine, mock_store):
    """Test that a pair can be registered with the service."""
    # Setup
    question = "test question"
    sql = "SELECT 1"
    tenant_id = 1
    roles = ["example"]

    # Execute
    pair = await RegistryService.register_pair(question, sql, tenant_id, roles)

    # Verify
    assert pair.signature_key == "SIG_KEY"
    assert pair.fingerprint == "FINGERPRINT"
    assert pair.question == question
    assert pair.sql_query == sql
    assert pair.roles == roles

    mock_store.store_pair.assert_called_once()
    stored_pair = mock_store.store_pair.call_args[0][0]
    assert isinstance(stored_pair, QueryPair)
    assert stored_pair.signature_key == "SIG_KEY"


@pytest.mark.asyncio
async def test_lookup_canonical(mock_canonicalizer, mock_store):
    """Test looking up a pair by its canonical signature."""
    # Setup
    mock_store.lookup_by_signature.return_value = QueryPair(
        signature_key="SIG_KEY",
        tenant_id=1,
        fingerprint="FINGERPRINT",
        question="test question",
        sql_query="SELECT 1",
        roles=["cache"],
    )

    # Execute
    pair = await RegistryService.lookup_canonical("test question", 1)

    # Verify
    assert pair is not None
    assert pair.signature_key == "SIG_KEY"
    mock_store.lookup_by_signature.assert_called_with("SIG_KEY", 1)


@pytest.mark.asyncio
async def test_get_few_shot_examples(mock_store, mock_rag_engine):
    """Test retrieving verified few-shot examples."""
    # Setup
    mock_store.lookup_semantic_candidates.return_value = [
        QueryPair(
            signature_key="SIG1",
            tenant_id=1,
            fingerprint="F1",
            question="Q1",
            sql_query="S1",
            roles=["example"],
            status="verified",
        ),
        QueryPair(
            signature_key="SIG2",
            tenant_id=1,
            fingerprint="F2",
            question="Q2",
            sql_query="S2",
            roles=["example"],
            status="unverified",
        ),
    ]

    # Execute
    examples = await RegistryService.get_few_shot_examples("test", 1, limit=3)

    # Verify: only verified ones should be returned
    assert len(examples) == 1
    assert examples[0].signature_key == "SIG1"
    mock_store.lookup_semantic_candidates.assert_called_once()
    # Check role filter was passed to DAL
    kwargs = mock_store.lookup_semantic_candidates.call_args[1]
    assert kwargs["role"] == "example"
