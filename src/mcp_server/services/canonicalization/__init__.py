"""Linguistic canonicalization service for deterministic cache keying.

This package provides SpaCy-based NLP processing to extract constraints
from natural language queries and generate semantic fingerprints for
exact-match cache lookup.
"""

from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService

__all__ = ["CanonicalizationService"]
