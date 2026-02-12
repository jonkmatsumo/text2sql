"""SQL AST complexity scoring helpers for MCP guardrails."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import sqlglot
from sqlglot import exp

from common.config.env import get_env_bool, get_env_int


@dataclass(frozen=True)
class ComplexityMetrics:
    """Computed SQL complexity metrics."""

    joins: int
    ctes: int
    subquery_depth: int
    has_cartesian: bool
    projection_count: int | None
    score: int


@dataclass(frozen=True)
class ComplexityLimits:
    """Configurable SQL complexity thresholds."""

    max_joins: int
    max_ctes: int
    max_subquery_depth: int
    disallow_cartesian: bool
    max_complexity_score: int
    max_projection_count: int | None


@dataclass(frozen=True)
class ComplexityViolation:
    """First triggered complexity limit."""

    limit_name: str
    measured: int | bool
    limit: int | bool


def _safe_env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        value = get_env_int(name, default)
    except ValueError:
        value = default
    if value is None:
        value = default
    return max(minimum, int(value))


def _safe_env_optional_int(name: str) -> Optional[int]:
    try:
        value = get_env_int(name, None)
    except ValueError:
        return None
    if value is None:
        return None
    parsed = int(value)
    return parsed if parsed > 0 else None


def get_mcp_complexity_limits() -> ComplexityLimits:
    """Resolve MCP complexity limits from environment variables."""
    try:
        disallow_cartesian = bool(get_env_bool("MCP_DISALLOW_CARTESIAN", True))
    except ValueError:
        disallow_cartesian = True

    return ComplexityLimits(
        max_joins=_safe_env_int("MCP_MAX_JOINS", 8, minimum=0),
        max_ctes=_safe_env_int("MCP_MAX_CTES", 10, minimum=0),
        max_subquery_depth=_safe_env_int("MCP_MAX_SUBQUERY_DEPTH", 4, minimum=0),
        disallow_cartesian=disallow_cartesian,
        max_complexity_score=_safe_env_int("MCP_MAX_COMPLEXITY_SCORE", 30, minimum=0),
        max_projection_count=_safe_env_optional_int("MCP_MAX_SELECT_PROJECTIONS"),
    )


def analyze_sql_complexity(sql: str, *, dialect: str = "postgres") -> ComplexityMetrics:
    """Parse SQL and compute complexity metrics."""
    expression = sqlglot.parse_one(sql, read=dialect)
    return compute_complexity_metrics(expression)


def compute_complexity_metrics(expression: exp.Expression) -> ComplexityMetrics:
    """Compute complexity metrics from a parsed sqlglot expression."""
    joins = _count_joins(expression)
    ctes = len(tuple(expression.find_all(exp.CTE)))
    subquery_depth = _max_subquery_depth(expression)
    has_cartesian = _has_cartesian_join(expression)
    projection_count = _projection_count(expression)
    score = _complexity_score(
        joins=joins,
        ctes=ctes,
        subquery_depth=subquery_depth,
        has_cartesian=has_cartesian,
        projection_count=projection_count,
    )
    return ComplexityMetrics(
        joins=joins,
        ctes=ctes,
        subquery_depth=subquery_depth,
        has_cartesian=has_cartesian,
        projection_count=projection_count,
        score=score,
    )


def find_complexity_violation(
    metrics: ComplexityMetrics, limits: ComplexityLimits
) -> Optional[ComplexityViolation]:
    """Return the first triggered complexity limit, if any."""
    checks: tuple[tuple[str, int | bool, int | bool, bool], ...] = (
        ("joins", metrics.joins, limits.max_joins, metrics.joins > limits.max_joins),
        ("ctes", metrics.ctes, limits.max_ctes, metrics.ctes > limits.max_ctes),
        (
            "subquery_depth",
            metrics.subquery_depth,
            limits.max_subquery_depth,
            metrics.subquery_depth > limits.max_subquery_depth,
        ),
        (
            "cartesian_join",
            metrics.has_cartesian,
            limits.disallow_cartesian,
            limits.disallow_cartesian and metrics.has_cartesian,
        ),
    )
    for limit_name, measured, limit, triggered in checks:
        if triggered:
            return ComplexityViolation(limit_name=limit_name, measured=measured, limit=limit)

    if (
        limits.max_projection_count is not None
        and metrics.projection_count is not None
        and metrics.projection_count > limits.max_projection_count
    ):
        return ComplexityViolation(
            limit_name="projection_count",
            measured=metrics.projection_count,
            limit=limits.max_projection_count,
        )

    if metrics.score > limits.max_complexity_score:
        return ComplexityViolation(
            limit_name="complexity_score",
            measured=metrics.score,
            limit=limits.max_complexity_score,
        )
    return None


def _count_joins(expression: exp.Expression) -> int:
    joins = len(tuple(expression.find_all(exp.Join)))
    # Some dialect/parser combinations may keep implicit comma tables in from_.expressions.
    implicit_joins = 0
    for select in expression.find_all(exp.Select):
        from_clause = select.args.get("from_")
        if from_clause is None:
            continue
        extra_from_entries = max(0, len(from_clause.expressions) - 1)
        implicit_joins += extra_from_entries
    return joins + implicit_joins


def _is_join_cartesian(join_node: exp.Join) -> bool:
    kind = str(join_node.args.get("kind") or "").upper()
    method = str(join_node.args.get("method") or "").upper()
    if kind == "CROSS":
        return True
    if method == "NATURAL":
        return False
    has_on = join_node.args.get("on") is not None
    using_values = join_node.args.get("using")
    has_using = isinstance(using_values, list) and len(using_values) > 0
    return not has_on and not has_using


def _has_cartesian_join(expression: exp.Expression) -> bool:
    return any(_is_join_cartesian(join_node) for join_node in expression.find_all(exp.Join))


def _children(node: exp.Expression) -> Iterable[exp.Expression]:
    for value in node.args.values():
        if isinstance(value, exp.Expression):
            yield value
            continue
        if isinstance(value, list):
            for item in value:
                if isinstance(item, exp.Expression):
                    yield item


def _max_subquery_depth(node: exp.Expression, depth: int = 0) -> int:
    current_depth = depth + 1 if isinstance(node, exp.Subquery) else depth
    max_depth = current_depth
    for child in _children(node):
        child_depth = _max_subquery_depth(child, current_depth)
        if child_depth > max_depth:
            max_depth = child_depth
    return max_depth


def _projection_count(expression: exp.Expression) -> int | None:
    top_level_select = (
        expression if isinstance(expression, exp.Select) else expression.find(exp.Select)
    )
    if not isinstance(top_level_select, exp.Select):
        return None
    return len(top_level_select.expressions or [])


def _complexity_score(
    *,
    joins: int,
    ctes: int,
    subquery_depth: int,
    has_cartesian: bool,
    projection_count: int | None,
) -> int:
    # Weights emphasize join fanout and nested subqueries; projections are a mild signal.
    score = (joins * 3) + (ctes * 2) + (subquery_depth * 4)
    if has_cartesian:
        score += 10
    if projection_count is not None:
        score += max(0, projection_count - 10)
    return score
