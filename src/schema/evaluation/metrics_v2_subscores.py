from sqlglot import exp

from src.schema.evaluation.metrics_v2_extractors import (
    extract_date_predicates,
    extract_equality_predicates,
    extract_in_lists,
    extract_limit_value,
    extract_numeric_predicates,
)


def numeric_range_similarity(gen_ast: exp.Expression, exp_ast: exp.Expression) -> float:
    """Compute similarity based on numeric predicates."""
    gen_pred = extract_numeric_predicates(gen_ast)
    exp_pred = extract_numeric_predicates(exp_ast)

    if not exp_pred:
        return 1.0 if not gen_pred else 0.0
    if not gen_pred:
        return 0.0

    # Group by column and operator for matching
    def group_predicates(preds):
        grouped = {}
        for col, op, val in preds:
            key = (col, op)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(val)
        return grouped

    gen_groups = group_predicates(gen_pred)
    exp_groups = group_predicates(exp_pred)

    total_score = 0.0
    match_count = 0

    for key, exp_vals in exp_groups.items():
        match_count += 1
        if key in gen_groups:
            gen_vals = gen_groups[key]
            # Simple heuristic: match the closest values if multiple exist
            # For brevity in V1, we just take the first one or average
            # Let's match each expected value to its closest generated value
            col_score = 0.0
            for ev in exp_vals:
                best_match = min(gen_vals, key=lambda gv: abs(gv - ev))
                denom = max(abs(ev), abs(best_match), 1.0)
                col_score += max(0.0, 1.0 - abs(ev - best_match) / denom)
            total_score += col_score / len(exp_vals)
        else:
            total_score += 0.0

    return total_score / match_count if match_count > 0 else 1.0


def date_range_similarity(gen_ast: exp.Expression, exp_ast: exp.Expression) -> float:
    """Compute similarity for date predicates."""
    gen_pred = extract_date_predicates(gen_ast)
    exp_pred = extract_date_predicates(exp_ast)

    if not exp_pred:
        return 1.0 if not gen_pred else 0.0
    if not gen_pred:
        return 0.0

    def group_predicates(preds):
        grouped = {}
        for col, op, val in preds:
            key = (col, op)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(val)
        return grouped

    gen_groups = group_predicates(gen_pred)
    exp_groups = group_predicates(exp_pred)

    total_score = 0.0
    match_count = 0

    for key, exp_vals in exp_groups.items():
        match_count += 1
        if key in gen_groups:
            gen_vals = gen_groups[key]
            col_score = 0.0
            for ev in exp_vals:
                # Find closest date
                best_match = min(gen_vals, key=lambda gv: abs((gv - ev).days))
                diff_days = abs((ev - best_match).days)
                # Normalize by a 365-day window for reasonable penalty
                col_score += max(0.0, 1.0 - diff_days / 365.0)
            total_score += col_score / len(exp_vals)
        else:
            total_score += 0.0

    return total_score / match_count if match_count > 0 else 1.0


def set_overlap_similarity(gen_ast: exp.Expression, exp_ast: exp.Expression) -> float:
    """Compute similarity for IN lists based on Jaccard overlap."""
    gen_pred = extract_in_lists(gen_ast)
    exp_pred = extract_in_lists(exp_ast)

    if not exp_pred:
        return 1.0 if not gen_pred else 0.0
    if not gen_pred:
        return 0.0

    # Group sets by column
    def group_sets(preds):
        grouped = {}
        for col, values in preds:
            if col not in grouped:
                grouped[col] = []
            grouped[col].append(values)
        return grouped

    gen_groups = group_sets(gen_pred)
    exp_groups = group_sets(exp_pred)

    total_score = 0.0
    match_count = 0

    for col, exp_sets in exp_groups.items():
        match_count += 1
        if col in gen_groups:
            gen_sets = gen_groups[col]
            # Match each expected set to the best generated set for that column
            col_score = 0.0
            for es in exp_sets:
                best_jaccard = 0.0
                for gs in gen_sets:
                    intersection = len(es & gs)
                    union = len(es | gs)
                    jaccard = intersection / union if union > 0 else 0.0
                    if jaccard > best_jaccard:
                        best_jaccard = jaccard
                col_score += best_jaccard
            total_score += col_score / len(exp_sets)
        else:
            total_score += 0.0

    return total_score / match_count if match_count > 0 else 1.0


def equality_value_match(gen_ast: exp.Expression, exp_ast: exp.Expression) -> float:
    """Compute similarity for equality predicates."""
    gen_pred = extract_equality_predicates(gen_ast)
    exp_pred = extract_equality_predicates(exp_ast)

    if not exp_pred:
        return 1.0 if not gen_pred else 0.0
    if not gen_pred:
        return 0.0

    def group_eq(preds):
        grouped = {}
        for col, val in preds:
            if col not in grouped:
                grouped[col] = []
            grouped[col].append(val)
        return grouped

    gen_groups = group_eq(gen_pred)
    exp_groups = group_eq(exp_pred)

    total_score = 0.0
    match_count = 0

    for col, exp_vals in exp_groups.items():
        match_count += 1
        if col in gen_groups:
            gen_vals = gen_groups[col]
            col_score = 0.0
            for ev in exp_vals:
                col_score += 1.0 if ev in gen_vals else 0.0
            total_score += col_score / len(exp_vals)
        else:
            total_score += 0.0

    return total_score / match_count if match_count > 0 else 1.0


def limit_distance_score(gen_ast: exp.Expression, exp_ast: exp.Expression) -> float:
    """Compute similarity for LIMIT values."""
    gen_limit = extract_limit_value(gen_ast)
    exp_limit = extract_limit_value(exp_ast)

    if gen_limit is None and exp_limit is None:
        return 1.0
    if gen_limit is None or exp_limit is None:
        return 0.0

    if gen_limit == exp_limit:
        return 1.0

    diff = abs(gen_limit - exp_limit)
    max_limit = max(gen_limit, exp_limit, 1)
    return max(0.0, 1.0 - diff / max_limit)
