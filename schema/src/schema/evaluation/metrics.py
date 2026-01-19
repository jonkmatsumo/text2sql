import logging
from typing import Any, Dict, List, Optional, Set

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


class MetricSuiteV1:
    """
    SQL Evaluation Metrics Suite V1.

    Provides deterministic structural and exact-match scores for SQL comparison.
    """

    # Hardcoded weights (must sum to 1.0)
    WEIGHTS = {
        "table_overlap": 0.2,
        "join_similarity": 0.2,
        "aggregation_match": 0.15,
        "groupby_match": 0.15,
        "predicate_similarity": 0.2,
        "limit_match": 0.1,
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

        # 1. Exact Match via Canonicalization
        exact_match = cls.check_exact_match(generated_sql, expected_sql)

        # 2. Structural Subscores
        try:
            gen_ast = sqlglot.parse_one(generated_sql, read="postgres")
            exp_ast = sqlglot.parse_one(expected_sql, read="postgres")

            subscores = cls.compute_subscores(gen_ast, exp_ast)

            # Weighted aggregation
            structural_score = sum(subscores[k] * cls.WEIGHTS[k] for k in cls.WEIGHTS)

            return {
                "exact_match": exact_match,
                "structural_score": round(structural_score, 4),
                "subscores": {k: round(v, 4) for k, v in subscores.items()},
                "generated_tables": cls._get_tables(gen_ast),
                "expected_tables": cls._get_tables(exp_ast),
                "parse_errors": [],
            }

        except Exception as e:
            parse_errors = [str(e)]
            # If expected_sql fails to parse (shouldn't happen in golden),
            # we still return a failure result
            try:
                exp_ast = sqlglot.parse_one(expected_sql, read="postgres")
                expected_tables = cls._get_tables(exp_ast)
            except Exception:
                expected_tables = []
                parse_errors.append(f"Expected SQL parse error: {e}")

            return {
                "exact_match": False,
                "structural_score": 0.0,
                "subscores": {k: 0.0 for k in cls.WEIGHTS},
                "generated_tables": [],
                "expected_tables": expected_tables,
                "parse_errors": parse_errors,
            }

    @staticmethod
    def check_exact_match(sql1: str, sql2: str) -> bool:
        """Check if two SQL queries are exactly the same after canonicalization."""
        try:
            # Canonicalize using sqlglot
            # Lowercase canonicalized SQL to handle case-insensitive identifiers
            can1 = sqlglot.parse_one(sql1, read="postgres").sql(dialect="postgres").lower()
            can2 = sqlglot.parse_one(sql2, read="postgres").sql(dialect="postgres").lower()
            return can1 == can2
        except Exception:
            # Fallback to whitespace normalization if parsing fails
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
        gen_tables = set(cls._get_tables(gen))
        exp_tables = set(cls._get_tables(exp_ast))
        return cls._jaccard(gen_tables, exp_tables)

    @classmethod
    def _score_join_similarity(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        gen_joins = {j.sql().lower() for j in gen.find_all(exp.Join)}
        exp_joins = {j.sql().lower() for j in exp_ast.find_all(exp.Join)}
        return cls._jaccard(gen_joins, exp_joins)

    @classmethod
    def _score_aggregation_match(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        agg_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)

        def get_aggs(ast):
            # Map of agg_type_name -> count
            counts = {}
            for node in ast.find_all(agg_types):
                name = type(node).__name__
                counts[name] = counts.get(name, 0) + 1
            return counts

        gen_aggs = get_aggs(gen)
        exp_aggs = get_aggs(exp_ast)

        if not gen_aggs and not exp_aggs:
            return 1.0

        all_keys = set(gen_aggs.keys()) | set(exp_aggs.keys())
        matches = 0
        for k in all_keys:
            matches += min(gen_aggs.get(k, 0), exp_aggs.get(k, 0))

        total = sum(max(gen_aggs.get(k, 0), exp_aggs.get(k, 0)) for k in all_keys)
        return matches / total if total > 0 else 1.0

    @classmethod
    def _score_groupby_match(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        def get_group_cols(ast):
            group = ast.find(exp.Group)
            if not group:
                return set()
            return {col.sql().lower() for col in group.find_all(exp.Column)}

        gen_cols = get_group_cols(gen)
        exp_cols = get_group_cols(exp_ast)
        return cls._jaccard(gen_cols, exp_cols)

    @classmethod
    def _score_predicate_similarity(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        def get_predicates(ast):
            where = ast.find(exp.Where)
            if not where:
                return set()
            # This is a bit naive, but we'll extract individual binary expressions
            predicates = set()
            for pred in where.find_all((exp.Binary, exp.In, exp.Between)):
                predicates.add(pred.sql().lower())
            return predicates

        gen_preds = get_predicates(gen)
        exp_preds = get_predicates(exp_ast)
        return cls._jaccard(gen_preds, exp_preds)

    @classmethod
    def _score_limit_match(cls, gen: exp.Expression, exp_ast: exp.Expression) -> float:
        def get_limit(ast):
            limit = ast.find(exp.Limit)
            if not limit:
                return None
            return limit.expression.sql().lower()

        gen_limit = get_limit(gen)
        exp_limit = get_limit(exp_ast)

        if gen_limit == exp_limit:
            return 1.0
        return 0.0

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
