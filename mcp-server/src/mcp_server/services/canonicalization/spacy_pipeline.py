"""SpaCy pipeline for linguistic canonicalization.

Provides deterministic extraction of constraints from natural language
queries using EntityRuler and DependencyMatcher.
"""

import hashlib
import logging
from pathlib import Path
from typing import Optional, Tuple

from common.config.env import get_env_bool, get_env_str
from common.telemetry import Telemetry

logger = logging.getLogger(__name__)

# Feature flag for gradual rollout
SPACY_ENABLED = get_env_bool("SPACY_ENABLED", False)


class CanonicalizationService:
    """Linguistic canonicalization using SpaCy NLP.

    Extracts constraints from natural language queries and generates
    deterministic semantic fingerprints for cache keying.

    Usage:
        service = CanonicalizationService.get_instance()
        constraints = service.extract_constraints("Top 10 PG movies")
        fingerprint = service.generate_fingerprint(constraints)
        key = service.compute_fingerprint_key(fingerprint)
    """

    _instance: Optional["CanonicalizationService"] = None
    _initialized: bool = False

    class PipelineState:
        """Immutable container for thread-safe atomic swapping."""

        def __init__(self, nlp, matcher):
            """Initialize pipeline state."""
            self.nlp = nlp
            self.matcher = matcher

    def __init__(self, model: str = "en_core_web_sm"):
        """Initialize SpaCy pipeline with EntityRuler and DependencyMatcher.

        Args:
            model: SpaCy model name (default: en_core_web_sm for speed)
        """
        self._state: Optional[CanonicalizationService.PipelineState] = None
        if CanonicalizationService._initialized:
            return

        self.model = model

        try:
            import spacy  # noqa: F401
        except ImportError:
            logger.warning("SpaCy not installed. Canonicalization disabled.")
            return

        self._state = self._build_pipeline(model)

        CanonicalizationService._initialized = True
        if self._state:
            logger.info(f"CanonicalizationService initialized with model: {model}")

    @property
    def nlp(self):
        """Access SpaCy NLP object from current state."""
        return self._state.nlp if self._state else None

    @property
    def matcher(self):
        """Access DependencyMatcher from current state."""
        return self._state.matcher if self._state else None

    def _build_pipeline(self, model: str, extra_patterns: list = None) -> Optional[PipelineState]:
        """Build a fresh pipeline state.

        Args:
            model: Name of spacy model to load
            extra_patterns: Optional list of patterns to add to EntityRuler

        Returns:
            PipelineState or None if loading fails
        """
        import spacy

        try:
            nlp = spacy.load(model)
        except OSError:
            logger.warning(
                f"SpaCy model '{model}' not found. Run: python -m spacy download {model}"
            )
            return None

        custom_patterns_count = self._setup_entity_ruler(nlp, extra_patterns)

        # Hardening: Ensure count is an integer (protects against bad mocks/tests)
        if not isinstance(custom_patterns_count, int):
            logger.debug(
                f"Expected int from _setup_entity_ruler, got {type(custom_patterns_count)}. "
                "Treating as 0."
            )
            custom_patterns_count = 0

        matcher = self._setup_dependency_matcher(
            nlp, has_custom_patterns=(custom_patterns_count > 0)
        )

        return self.PipelineState(nlp, matcher)

    @classmethod
    def get_instance(cls) -> "CanonicalizationService":
        """Get singleton instance of the service."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton (for testing)."""
        cls._instance = None
        cls._initialized = False

    def _setup_entity_ruler(self, nlp, extra_patterns: list = None) -> int:
        """Load entity patterns from JSONL files and optional extra patterns.

        Returns:
            int: Total number of custom patterns loaded (file + extra)
        """
        # Add entity ruler before NER
        ruler = nlp.add_pipe("entity_ruler", before="ner")

        # Load patterns from files
        # Priority: Env Var -> /app/patterns -> local dev project paths -> package path
        env_path = get_env_str("PATTERNS_DIR")
        docker_path = Path("/app/patterns")
        dev_path = (
            Path(__file__).parent.parent.parent.parent.parent.parent
            / "database/query-target/patterns"
        )
        package_path = Path(__file__).parent.parent.parent / "patterns"

        total_custom_patterns = 0

        if env_path and Path(env_path).exists():
            patterns_dir = Path(env_path)
            logger.info(f"Loading patterns from env path: {patterns_dir}")
        elif docker_path.exists():
            patterns_dir = docker_path
            logger.info(f"Loading patterns from Docker path: {patterns_dir}")
        elif dev_path.exists():
            patterns_dir = dev_path
            logger.info(f"Loading patterns from dev path: {patterns_dir}")
        else:
            patterns_dir = package_path
            logger.info(f"Loading patterns from package path: {patterns_dir}")

        if patterns_dir.exists():
            import srsly

            all_patterns = []
            for pattern_file in patterns_dir.glob("*.jsonl"):
                try:
                    patterns = list(srsly.read_jsonl(pattern_file))
                    all_patterns.extend(patterns)
                    logger.info(f"Loaded {len(patterns)} patterns from {pattern_file.name}")
                except Exception as e:
                    logger.warning(f"Failed to load {pattern_file}: {e}")

            if all_patterns:
                ruler.add_patterns(all_patterns)
                total_custom_patterns += len(all_patterns)
                logger.info(f"Total file entity patterns loaded: {len(all_patterns)}")
        else:
            logger.warning(f"Patterns directory not found at {patterns_dir}")

        # Add extra patterns (e.g. from DB)
        if extra_patterns:
            ruler.add_patterns(extra_patterns)
            total_custom_patterns += len(extra_patterns)
            logger.info(f"Added {len(extra_patterns)} extra patterns")

        return total_custom_patterns

    def _setup_dependency_matcher(self, nlp, has_custom_patterns: bool = False):
        """Register structural patterns for constraint extraction.

        Args:
            nlp: SpaCy NLP object
            has_custom_patterns: Whether custom entity patterns were loaded
        """
        from mcp_server.services.canonicalization.dependency_patterns import (
            ENTITY_PATTERNS,
            LIMIT_PATTERNS,
            RATING_PATTERNS,
        )
        from spacy.matcher import DependencyMatcher

        # P2: Warn if using default patterns only (domain mismatch risk)
        if not has_custom_patterns:
            logger.warning(
                "event=nlp_dependency_patterns_default_only "
                "patterns_source=default_only "
                "domain_assumption=film_schema "
                "recommended_action='Provide domain-specific patterns file "
                "via PATTERNS_DIR env var'"
            )

        matcher = DependencyMatcher(nlp.vocab)
        matcher.add("RATING_CONSTRAINT", RATING_PATTERNS)
        matcher.add("LIMIT_CONSTRAINT", LIMIT_PATTERNS)
        matcher.add("ENTITY_CONSTRAINT", ENTITY_PATTERNS)
        return matcher

    async def reload_patterns(self) -> int:
        """Reload entity patterns from the database and atomic swap pipeline."""
        if not SPACY_ENABLED:
            return 0

        from dal.database import Database

        logger.info("Reloading patterns from database...")
        patterns = []

        # Use a new connection or the pool
        async with Database.get_connection() as conn:
            # Check if table exists first (in case running before migration,
            # though ensure_schema should handle it).
            # But typically we just query.
            rows = await conn.fetch("SELECT label, pattern, id FROM nlp_patterns")

            for row in rows:
                patterns.append(
                    {
                        "label": row["label"],
                        "pattern": row["pattern"],
                        "id": row["id"],
                    }
                )

        # Build NEW pipeline with DB patterns
        # This is the "Atomic Swap" preparation - heavy lifting done on local var
        new_state = self._build_pipeline(self.model, extra_patterns=patterns)

        if new_state:
            # Atomic swap
            self._state = new_state
            logger.info(f"Swapped pipeline with {len(patterns)} DB patterns.")
            return len(patterns)
        else:
            logger.error("Failed to build new pipeline during reload.")
            raise RuntimeError("Failed to build pipeline")

    def is_available(self) -> bool:
        """Check if SpaCy is properly initialized."""
        return self._state is not None and SPACY_ENABLED

    def extract_constraints(self, query: str) -> dict:
        """Extract constraints from natural language query.

        Args:
            query: Raw user query (e.g., "Top 10 PG rated movies")

        Returns:
            dict with keys: rating, limit, entity, metric, negated, confidence
        """
        constraints = {
            "rating": None,
            "limit": None,
            "entity": None,
            "metric": None,
            "negated": False,
            "confidence": 0.0,
        }

        # Local atomic reference
        state = self._state
        if not state or not SPACY_ENABLED:
            return constraints

        doc = state.nlp(query)

        # Extract from named entities (EntityRuler)
        for ent in doc.ents:
            if ent.label_ == "RATING":
                constraints["rating"] = ent.ent_id_ or ent.text.upper()
                constraints["confidence"] += 0.4
                # Check for negation
                if any(child.dep_ == "neg" for child in ent.root.children):
                    constraints["negated"] = True
            elif ent.label_ == "AGGREGATOR":
                constraints["metric"] = ent.ent_id_ or ent.text.upper()
                constraints["confidence"] += 0.2
            elif ent.label_ == "ENTITY":
                constraints["entity"] = ent.ent_id_ or ent.text.upper()
                constraints["confidence"] += 0.2

        # Extract from dependency patterns
        if state.matcher:
            matches = state.matcher(doc)
            for match_id, token_ids in matches:
                pattern_name = state.nlp.vocab.strings[match_id]

                if pattern_name == "LIMIT_CONSTRAINT":
                    for idx in token_ids:
                        token = doc[idx]
                        if token.pos_ == "NUM":
                            try:
                                constraints["limit"] = int(token.text)
                                constraints["confidence"] += 0.2
                            except ValueError:
                                pass

                elif pattern_name == "ENTITY_CONSTRAINT":
                    for idx in token_ids:
                        token = doc[idx]
                        lemma = token.lemma_.lower()
                        if lemma in ("movie", "film", "show", "video"):
                            constraints["entity"] = "FILM"
                        elif lemma in ("actor", "actress", "performer"):
                            constraints["entity"] = "ACTOR"
                        elif lemma in ("merchant", "store", "vendor"):
                            constraints["entity"] = "MERCHANT"
                        elif lemma in ("transaction", "payment", "purchase"):
                            constraints["entity"] = "TRANSACTION"
                        elif lemma in ("account", "balance"):
                            constraints["entity"] = "ACCOUNT"
                        elif lemma in ("institution", "bank"):
                            constraints["entity"] = "INSTITUTION"
                        elif lemma in ("customer", "client", "user"):
                            constraints["entity"] = "CUSTOMER"

                elif pattern_name == "RATING_CONSTRAINT":
                    # Pattern matches [film] -> [rated] -> [PG] or similar
                    for idx in token_ids:
                        token = doc[idx]
                        # Heuristic: Rating is usually upper case or specific values like 'G', 'R'
                        if token.text.upper() in ("G", "PG", "PG-13", "R", "NC-17", "NC17"):
                            constraints["rating"] = token.text.upper()
                            constraints["confidence"] += 0.3
                            break

        return constraints

    def generate_fingerprint(self, constraints: dict) -> str:
        """Generate deterministic semantic fingerprint.

        Args:
            constraints: Extracted constraint dict

        Returns:
            Canonical string like "FILTER:RATING=PG|LIMIT:10"
        """
        parts = []

        if constraints.get("entity"):
            parts.append(f"SELECT:{constraints['entity']}")
        if constraints.get("rating"):
            neg = "NOT_" if constraints.get("negated") else ""
            parts.append(f"FILTER:RATING={neg}{constraints['rating']}")
        if constraints.get("metric"):
            parts.append(f"AGG:{constraints['metric']}")
        if constraints.get("limit"):
            parts.append(f"LIMIT:{constraints['limit']}")
        if constraints.get("sort_direction"):
            parts.append(f"SORT:{constraints['sort_direction']}")

        # Sort for determinism
        return "|".join(sorted(parts))

    def compute_fingerprint_key(self, fingerprint: str) -> str:
        """Compute SHA256 hash of fingerprint for cache key.

        Args:
            fingerprint: Semantic fingerprint string

        Returns:
            64-character hex string
        """
        return hashlib.sha256(fingerprint.encode()).hexdigest()

    async def process_query(self, query: str) -> Tuple[dict, str, str]:
        """Full pipeline: extract constraints, generate fingerprint and key.

        Args:
            query: Raw user query

        Returns:
            tuple of (constraints, fingerprint, fingerprint_key)
        """
        with Telemetry.start_span(
            "canonicalize.spacy",
            attributes={
                "telemetry.input_len_chars": len(query),
                "spacy.model": self.model,
            },
        ) as span:
            try:
                constraints = self.extract_constraints(query)
                fingerprint = self.generate_fingerprint(constraints)
                key = self.compute_fingerprint_key(fingerprint)

                span.set_attribute("telemetry.output_len_chars", len(fingerprint))
                span.set_attribute("spacy.fingerprint", fingerprint)
                Telemetry.set_span_status(span, True)

                return constraints, fingerprint, key
            except Exception as e:
                Telemetry.set_span_status(span, False, e)
                raise
