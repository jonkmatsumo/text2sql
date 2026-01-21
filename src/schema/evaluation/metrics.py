import logging
from typing import Any, Dict, List, Optional, Set

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


class MetricSuiteV1:
    """
    SQL Evaluation Metrics Suite V1.

    Provides deterministic structural and exact-match scores for SQL comparison.
    Follows the locked Metrics V1 Spec.
    """

    # Locked Weights (Must sum to 1.0)
    WEIGHTS = {
        "table_overlap": 0.35,
        "join_similarity": 0.15,
        "aggregation_match": 0.15,
        "groupby_match": 0.10,
        "predicate_similarity": 0.15,
        "limit_match": 0.10,
    }

    @classmethod
    def compute_all(cls, generated_sql: Optional[str], expected_sql: str) -> Dict[str, Any]:
        """
        Compute all metrics for a generated SQL vs expected SQL.

        Returns:
            Dict containing:
                - exact_match: bool
                - structural_score: float (0.0 - 1.0)
                - subscores: Dict[str, float]
                - generated_tables: List[str]
                - expected_tables: List[str]
                - parse_errors: List[str]
        """
        if not generated_sql:
            return cls._empty_metrics(expected_sql, ["Missing generated SQL"])

        # 1. Exact Match via Canonicalization and Parse Failure Handling
        exact_match, gen_ast, exp_ast, parse_errors = cls._compute_exact_match_and_asts(
            generated_sql, expected_sql
        )

        # 2. Structural Subscores
        # if either parse fails: structural_score = 1.0 if exact_match True else 0.0
        if gen_ast is None or exp_ast is None:
            structural_score = 1.0 if exact_match else 0.0
            subscores = {k: 1.0 if exact_match else 0.0 for k in cls.WEIGHTS}
        else:
            subscores = cls.compute_subscores(gen_ast, exp_ast)
            # Weighted aggregation
            structural_score = sum(subscores[k] * cls.WEIGHTS[k] for k in cls.WEIGHTS)

        return {
            "exact_match": exact_match,
            "structural_score": round(structural_score, 4),
            "subscores": {k: round(v, 4) for k, v in subscores.items()},
            "generated_tables": cls._get_tables(gen_ast) if gen_ast else [],
            "expected_tables": cls._get_tables(exp_ast) if exp_ast else [],
            "parse_errors": parse_errors,
        }

    @classmethod
    def _compute_exact_match_and_asts(
        cls, sql1: str, sql2: str
    ) -> tuple[bool, Optional[exp.Expression], Optional[exp.Expression], List[str]]:
        """
        Compute exact match and return ASTs if they parse.

        Follows the spec for parse failure behavior.
        """
        gen_ast = None
        exp_ast = None
        parse_errors = []

        try:
            gen_ast = sqlglot.parse_one(sql1, read="postgres")
        except Exception as e:
            parse_errors.append(f"Generated SQL parse error: {str(e)}")

        try:
            exp_ast = sqlglot.parse_one(sql2, read="postgres")
        except Exception as e:
            parse_errors.append(f"Expected SQL parse error: {str(e)}")

        # Case 1: Both parse
        if gen_ast is not None and exp_ast is not None:
            can1 = gen_ast.sql(dialect="postgres").lower()
            can2 = exp_ast.sql(dialect="postgres").lower()
            return (can1 == can2), gen_ast, exp_ast, parse_errors

        # Case 2: Both fail to parse
        if gen_ast is None and exp_ast is None:
            norm1 = " ".join(sql1.lower().split())
            norm2 = " ".join(sql2.lower().split())
            return (norm1 == norm2), None, None, parse_errors

        # Case 3: One fails to parse
        return False, gen_ast, exp_ast, parse_errors

    @staticmethod
    def check_exact_match(sql1: str, sql2: str) -> bool:
        """Lightweight wrapper for check_exact_match."""
        try:
            can1 = sqlglot.parse_one(sql1, read="postgres").sql(dialect="postgres").lower()
            can2 = sqlglot.parse_one(sql2, read="postgres").sql(dialect="postgres").lower()
            return can1 == can2
        except Exception:
            # Fallback for simple calls without full compute_all logic
            norm1 = " ".join(sql1.lower().split())
            norm2 = " ".join(sql2.lower().split())
            return norm1 == norm2

    @classmethod
    def compute_subscores(
        cls, gen_ast: exp.Expression, exp_ast: exp.Expression
    ) -> Dict[str, float]:
        """Compute individual structural subscores."""
        return {
            "table_overlap": cls._score_table_overlap(gen_ast, exp_ast),
            "join_similarity": cls._score_join_similarity(gen_ast, exp_ast),
            "aggregation_match": cls._score_aggregation_match(gen_ast, exp_ast),
            "groupby_match": cls._score_groupby_match(gen_ast, exp_ast),
            "predicate_similarity": cls._score_predicate_similarity(gen_ast, exp_ast),
            "limit_match": cls._score_limit_match(gen_ast, exp_ast),
        }

    @staticmethod
    def _get_tables(ast: exp.Expression) -> List[str]:
        """Extract table names from AST."""
        tables = set()
        for table in ast.find_all(exp.Table):
            if table.name:
                name = table.name.lower()
                if table.db:
                    name = f"{table.db.lower()}.{name}"
                tables.add(name)
        return sorted(list(tables))

    @staticmethod
    def _jaccard(set1: Set[Any], set2: Set[Any]) -> float:
        """Compute Jaccard similarity between two sets."""
        if not set1 and not set2:
            return 1.0
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union

    @classmethod
    def _score_table_overlap(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        """Compute table overlap similarity."""
        gen_tables = set(cls._get_tables(gen))
        exp_tables = set(cls._get_tables(exp_ast))
        return cls._jaccard(gen_tables, exp_tables)

    @classmethod
    def _score_join_similarity(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        """Compute normalized join count difference."""
        gen_count = len(list(gen.find_all(exp.Join)))
        exp_count = len(list(exp_ast.find_all(exp.Join)))

        if gen_count == exp_count:
            return 1.0

        diff = abs(gen_count - exp_count)
        max_count = max(gen_count, exp_count, 1)
        return max(0.0, 1.0 - diff / max_count)

    @classmethod
    def _score_aggregation_match(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        """Compute boolean aggregation presence match."""
        agg_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max, exp.AggFunc)

        gen_has_agg = gen.find(agg_types) is not None
        exp_has_agg = exp_ast.find(agg_types) is not None

        return 1.0 if gen_has_agg == exp_has_agg else 0.0

    @classmethod
    def _score_groupby_match(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        """Compute boolean GROUP BY presence match."""
        gen_has_gb = gen.find(exp.Group) is not None
        exp_has_gb = exp_ast.find(exp.Group) is not None

        return 1.0 if gen_has_gb == exp_has_gb else 0.0

    @classmethod
    def _score_predicate_similarity(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        """Compute predicate TYPE set Jaccard similarity."""

        def get_predicate_types(ast: exp.Expression) -> Set[str]:
            types = set()
            where = ast.find(exp.Where)
            if not where:
                return types

            # equality: EQ
            if where.find(exp.EQ):
                types.add("equality")

            # range: GT, GTE, LT, LTE, Between
            if where.find((exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Between)):
                types.add("range")

            # in: In
            if where.find(exp.In):
                types.add("in")

            # like: Like, ILike
            if where.find((exp.Like, exp.ILike)):
                types.add("like")

            # null_check: Is (IS NULL / IS NOT NULL)
            if where.find(exp.Is):
                types.add("null_check")

            return types

        gen_types = get_predicate_types(gen)
        exp_types = get_predicate_types(exp_ast)
        return cls._jaccard(gen_types, exp_types)

    @classmethod
    def _score_limit_match(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        """Compute LIMIT match score per spec."""

        def get_limit_val(ast: exp.Expression) -> Optional[int]:
            limit = ast.find(exp.Limit)
            if not limit:
                return None
            try:
                return int(limit.expression.name)
            except (ValueError, TypeError, AttributeError):
                return -1  # Sentinel if it's an expression like '10 + 5'

        gen_limit = get_limit_val(gen)
        exp_limit = get_limit_val(exp_ast)

        if gen_limit is None and exp_limit is None:
            return 1.0
        if gen_limit is None or exp_limit is None:
            return 0.0

        if gen_limit == exp_limit:
            return 1.0

        # Formula: max(0.0, 1.0 - |gen - exp| / max(gen, exp))
        # Handle sentinel values
        if gen_limit < 0 or exp_limit < 0:
            return 0.0

        diff = abs(gen_limit - exp_limit)
        max_limit = max(gen_limit, exp_limit, 1)
        return max(0.0, 1.0 - diff / max_limit)

    @classmethod
    def _empty_metrics(cls, expected_sql: str, errors: List[str]) -> Dict[str, Any]:
        """Return failure metrics when generated SQL is missing or empty."""
        try:
            exp_ast = sqlglot.parse_one(expected_sql, read="postgres")
            expected_tables = cls._get_tables(exp_ast)
        except Exception:
            expected_tables = []

        return {
            "exact_match": False,
            "structural_score": 0.0,
            "subscores": {k: 0.0 for k in cls.WEIGHTS},
            "generated_tables": [],
            "expected_tables": expected_tables,
            "parse_errors": errors,
        }
