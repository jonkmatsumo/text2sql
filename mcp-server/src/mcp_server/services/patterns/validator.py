"""Pattern validation service."""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from mcp_server.services.sanitization.text_sanitizer import sanitize_text


@dataclass
class ValidationFailure:
    """Structure for validation failures."""

    raw_pattern: str
    sanitized_pattern: Optional[str]
    reason: str
    details: str


class PatternValidator:
    """Validator for entity patterns."""

    def validate_batch(
        self,
        patterns: List[Dict[str, str]],
        existing_patterns: Optional[List[Dict[str, str]]] = None,
    ) -> Tuple[List[Dict[str, str]], List[ValidationFailure]]:
        """Validate a batch of patterns.

        Performs:
        1. Sanitization (using generic sanitizer).
        2. Deduplication within the batch.
        3. Conflict detection against existing patterns.

        Args:
            patterns: List of candidate patterns (dicts with label, pattern, id).
            existing_patterns: List of already approved patterns to check against.

        Returns:
            Tuple of (valid_patterns, failures).
        """
        valid: List[Dict[str, str]] = []
        failures: List[ValidationFailure] = []

        # Track seen in this batch to detect duplicates within run
        # (DUP_WITHIN_LABEL, DUP_CROSS_LABEL)
        # Key: sanitized_pattern -> (label, id)
        seen_in_batch: Dict[str, Tuple[str, str]] = {}

        # Track existing patterns (conceptually "persisted")
        # Key: sanitized_pattern -> (label, id)
        known_patterns: Dict[str, Tuple[str, str]] = {}

        # Index existing patterns (lazy sanitation assumption: we verify them anyway to be safe)
        if existing_patterns:
            for p in existing_patterns:
                raw_ex = p.get("pattern", "")
                res_ex = sanitize_text(raw_ex)
                if res_ex.is_valid and res_ex.sanitized:
                    known_patterns[res_ex.sanitized] = (p.get("label", ""), p.get("id", ""))

        for p in patterns:
            raw = p.get("pattern", "")
            label = p.get("label", "UNKNOWN")
            pid = p.get("id", "UNKNOWN")

            # 1. Sanitize
            res = sanitize_text(raw)
            if not res.is_valid:
                failures.append(
                    ValidationFailure(
                        raw_pattern=raw,
                        sanitized_pattern=None,
                        reason="SANITIZATION_FAILED",
                        details=", ".join(res.errors),
                    )
                )
                continue

            sanitized = res.sanitized
            if not sanitized:  # Should be covered by is_valid, but safe typing
                continue

            # 2. Check Conflicts with Existing
            if sanitized in known_patterns:
                ex_label, ex_id = known_patterns[sanitized]

                if ex_label != label:
                    failures.append(
                        ValidationFailure(
                            raw,
                            sanitized,
                            "DUP_EXISTING_CONFLICT",
                            f"Conflict with existing {ex_label} pattern ID {ex_id}",
                        )
                    )
                    continue
                elif ex_id != pid:
                    failures.append(
                        ValidationFailure(
                            raw,
                            sanitized,
                            "DUP_EXISTING_CONFLICT",
                            f"Ambiguous ID with existing pattern ID {ex_id}",
                        )
                    )
                    continue
                else:
                    # Exact duplicate (Same pattern, label, ID)
                    failures.append(
                        ValidationFailure(
                            raw, sanitized, "DUP_EXISTING_EXACT", "Pattern already exists"
                        )
                    )
                    continue

            # 3. Check Duplicates in Batch
            if sanitized in seen_in_batch:
                seen_label, seen_id = seen_in_batch[sanitized]
                if seen_label != label:
                    failures.append(
                        ValidationFailure(
                            raw,
                            sanitized,
                            "DUP_CROSS_LABEL",
                            f"Conflict with {seen_label} in current batch",
                        )
                    )
                elif seen_id != pid:
                    failures.append(
                        ValidationFailure(
                            raw,
                            sanitized,
                            "DUP_WITHIN_LABEL",
                            f"Ambiguous ID {seen_id} in current batch",
                        )
                    )
                else:
                    failures.append(
                        ValidationFailure(
                            raw, sanitized, "DUP_WITHIN_LABEL", "Duplicate pattern in current batch"
                        )
                    )
                continue

            # Success
            seen_in_batch[sanitized] = (label, pid)
            # Create a clean copy with sanitized pattern
            new_p = p.copy()
            new_p["pattern"] = sanitized
            valid.append(new_p)

        return valid, failures
