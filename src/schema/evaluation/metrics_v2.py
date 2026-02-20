from typing import Any, Dict, Optional

import sqlglot

from src.schema.evaluation.metrics import MetricSuiteV1
from src.schema.evaluation.metrics_v2_subscores import (
    date_range_similarity,
    equality_value_match,
    limit_distance_score,
    numeric_range_similarity,
    set_overlap_similarity,
)


class MetricSuiteV2:
    """
    SQL Evaluation Metrics Suite V2.

    Combines V1 structural similarity with V2 value-aware similarity.
    """

    # V2 Value-Aware Weights (Must sum to 1.0)
    V2_WEIGHTS = {
        "numeric_range_similarity": 0.25,
        "date_range_similarity": 0.25,
        "set_overlap_similarity": 0.20,
        "equality_value_match": 0.20,
        "limit_distance_score": 0.10,
    }

    # Composite weights
    ALPHA = 0.6  # V1 structural weight
    BETA = 0.4  # V2 value-aware weight

    @classmethod
    def compute_all(cls, generated_sql: Optional[str], expected_sql: str) -> Dict[str, Any]:
        """
        Compute all metrics (V1 + V2) for a generated SQL vs expected SQL.

        Returns:
            Dict containing V1 metrics plus V2 specific fields.
        """
        # 1. Base V1 Metrics
        results = MetricSuiteV1.compute_all(generated_sql, expected_sql)

        # 2. Add V2 Metadata
        results["metrics_version"] = "v2"

        # 3. Compute Value-Aware Score
        v2_subscores = {}
        if not generated_sql:
            results["structural_score_v2"] = 0.0
            results["value_aware_score"] = 0.0
            results["v2_subscores"] = {k: 0.0 for k in cls.V2_WEIGHTS}
            return results

        try:
            gen_ast = sqlglot.parse_one(generated_sql, read="postgres")
            exp_ast = sqlglot.parse_one(expected_sql, read="postgres")

            v2_subscores = {
                "numeric_range_similarity": numeric_range_similarity(gen_ast, exp_ast),
                "date_range_similarity": date_range_similarity(gen_ast, exp_ast),
                "set_overlap_similarity": set_overlap_similarity(gen_ast, exp_ast),
                "equality_value_match": equality_value_match(gen_ast, exp_ast),
                "limit_distance_score": limit_distance_score(gen_ast, exp_ast),
            }
        except Exception:
            # If parse fails for V2 calculation, use V1 matching logic or defaults
            # (V1 already handled parse errors for its score)
            v2_subscores = {k: results["exact_match"] for k in cls.V2_WEIGHTS}

        value_aware_score = sum(v2_subscores[k] * cls.V2_WEIGHTS[k] for k in cls.V2_WEIGHTS)

        # 4. Composite Score
        v1_score = results["structural_score"]
        composite_score = (cls.ALPHA * v1_score) + (cls.BETA * value_aware_score)

        # 5. Populate Results
        results["structural_score_v1"] = v1_score
        results["structural_score_v2"] = round(composite_score, 4)
        results["value_aware_score"] = round(value_aware_score, 4)
        results["v2_subscores"] = {k: round(v, 4) for k, v in v2_subscores.items()}

        return results
