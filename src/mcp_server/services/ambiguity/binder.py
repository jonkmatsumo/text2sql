"""Mention extraction and candidate binding for deterministic ambiguity resolution.

This module extracts relevant mentions from user queries and maps them to
candidate elements in the database schema context.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List

import spacy

from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService

logger = logging.getLogger(__name__)


from mcp_server.services.ambiguity.models.entities import Candidate, Mention


class MentionExtractor:
    """Extracts schema-relevant mentions from user queries."""

    def __init__(self):
        """Initialize with SpaCy nlp pipeline."""
        self.service = CanonicalizationService.get_instance()
        self.nlp = self.service.nlp if self.service else None

        if not self.nlp:
            try:
                self.nlp = spacy.load("en_core_web_sm")
            except Exception:
                logger.warning("SpaCy model not found for MentionExtractor.")

    def extract(self, query: str) -> List[Mention]:
        """Extract all plausible mentions from the query."""
        if not self.nlp:
            return []

        doc = self.nlp(query)
        mentions = []

        # 1. Extract Noun Chunks (Potential tables/columns)
        for chunk in doc.noun_chunks:
            # Filter and lemmatize
            meaningful_tokens = [
                t for t in chunk if not t.is_stop and t.pos_ in ("NOUN", "PROPN", "ADJ")
            ]
            if not meaningful_tokens:
                continue

            clean_text = " ".join([t.lemma_.lower() for t in meaningful_tokens])

            mentions.append(
                Mention(
                    text=clean_text,
                    type="noun_phrase",
                    start_char=chunk.start_char,
                    end_char=chunk.end_char,
                    metadata={"original_text": chunk.text},
                )
            )

        # 2. Extract Entities from EntityRuler (Canonical mappings)
        for ent in doc.ents:
            # If we already have a noun chunk covering this, we might want to merge or tag it
            # For now, append as 'entity' type if not already represented
            covered = False
            for m in mentions:
                if m.start_char <= ent.start_char and m.end_char >= ent.end_char:
                    m.type = "entity"
                    m.metadata["ent_type"] = ent.label_
                    m.metadata["ent_id"] = ent.ent_id_
                    covered = True
                    break

            if not covered:
                mentions.append(
                    Mention(
                        text=ent.text,
                        type="entity",
                        start_char=ent.start_char,
                        end_char=ent.end_char,
                        metadata={"ent_type": ent.label_, "ent_id": ent.ent_id_},
                    )
                )

        # 3. Handle intentional cues (metrics/filters) - Implementation stub
        # e.g., "top", "most", "average"

        return self._deduplicate_mentions(mentions)

    def _deduplicate_mentions(self, mentions: List[Mention]) -> List[Mention]:
        """Merge overlapping mentions, preferring more specific ones."""
        if not mentions:
            return []

        # Sort by length (desc) then start position
        sorted_mentions = sorted(mentions, key=lambda x: (len(x.text), -x.start_char), reverse=True)
        final = []

        for m in sorted_mentions:
            overlap = False
            for existing in final:
                if m.start_char < existing.end_char and m.end_char > existing.start_char:
                    overlap = True
                    break
            if not overlap:
                final.append(m)

        return sorted(final, key=lambda x: x.start_char)


class CandidateBinder:
    """Binds mentions to schema elements within a context."""

    def __init__(self):
        """Initialize binder.

        NOTE: RagEngine was removed - semantic scoring is no longer performed
        due to embedding model mismatch. See final_score() for details.
        """
        pass

    def get_candidates(self, mention: Mention, schema_context: List[Any]) -> List[Candidate]:
        """Enumerate and score candidates for a mention.

        If mention.metadata contains an 'ent_id' (canonical ID from ontology),
        we prioritize exact matches on that ID over lexical/semantic scoring.
        """
        # === Phase 1: Ontology-based short-circuit ===
        # If SpaCy EntityRuler provided a canonical ent_id, check for exact match first
        ent_id = mention.metadata.get("ent_id")
        if ent_id:
            # Search for exact match on table name or column ID
            for table in schema_context:
                # Check table ID match
                if table["name"].lower() == ent_id.lower():
                    return [
                        Candidate(
                            kind="table",
                            id=table["name"],
                            label=table.get("description", table["name"]),
                            scores={"ontology_match": 1.0, "lexical": 1.0, "relational": 1.0},
                            metadata={"table": table["name"], "matched_via": "ontology"},
                        )
                    ]
                # Check column ID match (format: table.column)
                for col in table.get("columns", []):
                    col_id = f"{table['name']}.{col['name']}"
                    if col_id.lower() == ent_id.lower():
                        return [
                            Candidate(
                                kind="column",
                                id=col_id,
                                label=col.get("description", col["name"]),
                                scores={"ontology_match": 1.0, "lexical": 1.0, "relational": 1.0},
                                metadata={
                                    "table": table["name"],
                                    "column": col["name"],
                                    "matched_via": "ontology",
                                },
                            )
                        ]
            # ent_id was provided but no match found in schema_context
            # Fall through to normal scoring, but mark that ontology lookup failed
            logger.debug(f"Ontology ent_id '{ent_id}' not found in schema_context, using fallback")

        # === Phase 2: Standard lexical/semantic scoring ===
        candidates = []
        mention_text = mention.text.lower()

        for table in schema_context:
            table_name = table["name"].lower()

            # 1. Table Matching
            table_score = self._calculate_lexical_score(mention_text, table_name)
            # Remove strict threshold to allow semantic recovery
            candidates.append(
                Candidate(
                    kind="table",
                    id=table["name"],
                    label=table.get("description", table["name"]),
                    scores={"lexical": table_score},
                    metadata={"table": table["name"]},
                )
            )

            # 2. Column Matching
            for col in table.get("columns", []):
                col_name = col["name"].lower()
                col_score = self._calculate_lexical_score(mention_text, col_name)

                # Check column description too
                desc_score = 0.0
                if col.get("description"):
                    if mention_text in col["description"].lower():
                        desc_score = 0.7

                final_lexical = max(col_score, desc_score)

                # Remove strict threshold to allow semantic recovery
                candidates.append(
                    Candidate(
                        kind="column",
                        id=f"{table['name']}.{col['name']}",
                        label=col.get("description", col["name"]),
                        scores={"lexical": final_lexical, "relational": 0.5},
                        metadata={
                            "table": table["name"],
                            "column": col["name"],
                            "is_primary_key": col.get("is_primary_key", False),
                            "is_foreign_key": col.get("is_foreign_key", False),
                        },
                    )
                )

            # 3. Value Matching (from Sample Data)
            if table.get("sample_data"):
                try:
                    samples = []
                    if isinstance(table["sample_data"], str):
                        samples = json.loads(table["sample_data"])
                    elif isinstance(table["sample_data"], list):
                        samples = table["sample_data"]

                    # We check only the first few rows to avoid perf hit
                    # though samples is usually small
                    for row in samples:
                        for col, val in row.items():
                            if not val or not isinstance(val, (str, int, float)):
                                continue

                            val_str = str(val).lower()
                            score = self._calculate_lexical_score(mention_text, val_str)

                            if score > 0.6:
                                candidates.append(
                                    Candidate(
                                        kind="value",
                                        id=f"{table['name']}.{col}='{val}'",
                                        label=f"Value '{val}' in {col}",
                                        scores={"lexical": score, "relational": 0.4},
                                        metadata={
                                            "table": table["name"],
                                            "column": col,
                                            "value": val,
                                        },
                                    )
                                )
                except Exception as e:
                    logger.debug(f"Failed to parse sample data for {table_name}: {e}")

        # Add table matching with boost
        for cand in candidates:
            if cand.kind == "table":
                cand.scores["relational"] = 1.0

        # NOTE: Semantic scoring was removed - see final_score comment for rationale

        return sorted(candidates, key=lambda x: x.final_score, reverse=True)

    def _calculate_lexical_score(self, m: str, target: str) -> float:
        """Improved lexical similarity score handling snake_case and word overlap."""
        m_clean = m.lower().strip()
        t_clean = target.lower().replace("_", " ").strip()

        if m_clean == t_clean:
            return 1.0

        # Check containment - Removed hardcoded 0.9 to rely on word overlap/fuzzy matching
        # if m_clean in t_clean or t_clean in m_clean:
        #    return 0.9

        # Word overlap (Jaccard-ish)
        m_words = set(m_clean.split())
        t_words = set(t_clean.split())
        intersection = m_words.intersection(t_words)

        if intersection:
            overlap_score = 0.7 + 0.2 * (len(intersection) / max(len(m_words), len(t_words)))
            return overlap_score

        from difflib import SequenceMatcher

        return SequenceMatcher(None, m_clean, t_clean).ratio()
