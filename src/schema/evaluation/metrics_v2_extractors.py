from datetime import date
from typing import List, Optional, Set, Tuple, Union

from sqlglot import exp


def extract_numeric_predicates(ast: exp.Expression) -> List[Tuple[str, str, float]]:
    """
    Extract numeric predicates (GT, GTE, LT, LTE) from WHERE/HAVING clauses.

    Returns: List of (column, operator, value)
    """
    results = []
    # Numeric comparison types
    numeric_ops = {
        exp.GT: ">",
        exp.GTE: ">=",
        exp.LT: "<",
        exp.LTE: "<=",
        exp.EQ: "=",
    }

    # Search for comparisons
    for node in ast.find_all(tuple(numeric_ops.keys())):
        left = node.left
        right = node.right

        # We look for Column on one side and Literal (Number) on the other
        column = None
        op = numeric_ops[type(node)]
        value = None

        if isinstance(left, exp.Column) and isinstance(right, exp.Literal) and right.is_number:
            column = left.name.lower()
            value = float(right.this)
        elif isinstance(right, exp.Column) and isinstance(left, exp.Literal) and left.is_number:
            column = right.name.lower()
            value = float(left.this)
            # Flip operator if sides are swapped
            flip = {">": "<", ">=": "<=", "<": ">", "<=": ">=", "=": "="}
            op = flip[op]

        if column and value is not None:
            results.append((column, op, value))

    # Also extract from BETWEEN
    for node in ast.find_all(exp.Between):
        if isinstance(node.this, exp.Column):
            column = node.this.name.lower()
            low = node.args.get("low")
            high = node.args.get("high")
            if (
                isinstance(low, exp.Literal)
                and low.is_number
                and isinstance(high, exp.Literal)
                and high.is_number
            ):
                results.append((column, ">=", float(low.this)))
                results.append((column, "<=", float(high.this)))

    return results


def extract_in_lists(ast: exp.Expression) -> List[Tuple[str, Set[Union[str, float]]]]:
    """
    Extract IN lists from WHERE/HAVING clauses.

    Returns: List of (column, set_of_literals)
    """
    results = []
    for node in ast.find_all(exp.In):
        if isinstance(node.this, exp.Column):
            column = node.this.name.lower()
            values = set()
            for item in node.expressions:
                if isinstance(item, exp.Literal):
                    if item.is_number:
                        values.add(float(item.this))
                    else:
                        values.add(item.this.lower())
            if values:
                results.append((column, values))
    return results


def extract_equality_predicates(ast: exp.Expression) -> List[Tuple[str, Union[str, float]]]:
    """
    Extract equality predicates (=) where value is a literal.

    Returns: List of (column, value)
    """
    results = []
    for node in ast.find_all(exp.EQ):
        left = node.left
        right = node.right

        column = None
        value = None

        if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
            column = left.name.lower()
            value = float(right.this) if right.is_number else right.this.lower()
        elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
            column = right.name.lower()
            value = float(left.this) if left.is_number else left.this.lower()

        if column and value is not None:
            results.append((column, value))
    return results


def extract_limit_value(ast: exp.Expression) -> Optional[int]:
    """Extract LIMIT value."""
    limit = ast.find(exp.Limit)
    if not limit:
        return None
    try:
        return int(limit.expression.name)
    except (ValueError, TypeError, AttributeError):
        return None


def extract_date_predicates(ast: exp.Expression) -> List[Tuple[str, str, date]]:
    """
    Extract date predicates. Parses string literals as dates where possible.

    Returns: List of (column, operator, date_obj)
    """
    results = []
    ops = {
        exp.GT: ">",
        exp.GTE: ">=",
        exp.LT: "<",
        exp.LTE: "<=",
        exp.EQ: "=",
    }

    def try_parse_date(val: str) -> Optional[date]:
        try:
            return date.fromisoformat(val)
        except (ValueError, TypeError):
            return None

    for node in ast.find_all(tuple(ops.keys())):
        left = node.left
        right = node.right

        column = None
        op = ops[type(node)]
        date_val = None

        if isinstance(left, exp.Column) and isinstance(right, exp.Literal) and right.is_string:
            column = left.name.lower()
            date_val = try_parse_date(right.this)
        elif isinstance(right, exp.Column) and isinstance(left, exp.Literal) and left.is_string:
            column = right.name.lower()
            date_val = try_parse_date(left.this)
            flip = {">": "<", ">=": "<=", "<": ">", "<=": ">=", "=": "="}
            op = flip[op]

        if column and date_val:
            results.append((column, op, date_val))

    # Also BETWEEN
    for node in ast.find_all(exp.Between):
        if isinstance(node.this, exp.Column):
            column = node.this.name.lower()
            low = node.args.get("low")
            high = node.args.get("high")
            if (
                isinstance(low, exp.Literal)
                and low.is_string
                and isinstance(high, exp.Literal)
                and high.is_string
            ):
                d_low = try_parse_date(low.this)
                d_high = try_parse_date(high.this)
                if d_low and d_high:
                    results.append((column, ">=", d_low))
                    results.append((column, "<=", d_high))

    return results
