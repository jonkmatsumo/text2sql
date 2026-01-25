import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class FieldType(Enum):
    """Enumeration of Vega-Lite field types."""

    NUMERIC = "quantitative"
    TEMPORAL = "temporal"
    CATEGORICAL = "nominal"


@dataclass
class FieldSpec:
    """Specification for a single field in the dataset."""

    name: str
    field_type: FieldType


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_temporal(value: Any) -> bool:
    # Basic ISO date check YYYY-MM-DD
    if not isinstance(value, str):
        return False
    # Simple regex for YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
    # This matches typical SQL string outputs for dates
    iso_date_pattern = r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?)?Z?$"
    return bool(re.match(iso_date_pattern, value))


def infer_fields(rows: List[Dict[str, Any]], sample_size: int = 100) -> List[FieldSpec]:
    """Infer field names and types from a list of row dictionaries."""
    if not rows:
        return []

    # Assume all rows have same keys
    keys = list(rows[0].keys())
    specs = []

    for key in keys:
        # Check type based on sample
        is_num = True
        is_temp = True

        sample = rows[:sample_size]
        valid_samples = 0

        for row in sample:
            val = row.get(key)
            if val is None:
                continue

            valid_samples += 1
            if not _is_numeric(val):
                is_num = False
            if not _is_temporal(val):
                is_temp = False

        if valid_samples == 0:
            # Default to categorical if all None
            field_type = FieldType.CATEGORICAL
        elif is_num:
            field_type = FieldType.NUMERIC
        elif is_temp:
            field_type = FieldType.TEMPORAL
        else:
            field_type = FieldType.CATEGORICAL

        specs.append(FieldSpec(name=key, field_type=field_type))

    return specs


def build_vega_lite_spec(
    rows: List[Dict[str, Any]], chart_hint: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Build a Vega-Lite specification from data rows."""
    if not rows or not isinstance(rows, list):
        return None

    # Defensive check for error objects or non-dict rows
    if not isinstance(rows[0], dict):
        return None

    fields = infer_fields(rows)

    if len(fields) != 2:
        return None

    f1, f2 = fields[0], fields[1]

    mark = None

    # Identify x and y
    # Priority: temporal > categorical > numeric for X axis usually?
    # Let's follow heuristics:
    # 2 cols:
    # - cat + num -> bar (cat on X/Y, num on Y/X)
    # - temp + num -> line (temp on X)
    # - num + num -> scatter

    x_field = None
    y_field = None

    # helper for matching types
    types = {f1.field_type, f2.field_type}

    if FieldType.CATEGORICAL in types and FieldType.NUMERIC in types:
        mark = "bar"
        # Find which is which
        cat_field = f1 if f1.field_type == FieldType.CATEGORICAL else f2
        num_field = f1 if f1.field_type == FieldType.NUMERIC else f2

        # Heuristic: if cat has many unique values, maybe horizontal bar?
        # For now standard vertical bar
        x_field = cat_field
        y_field = num_field

    elif FieldType.TEMPORAL in types and FieldType.NUMERIC in types:
        mark = "line"
        temp_field = f1 if f1.field_type == FieldType.TEMPORAL else f2
        num_field = f1 if f1.field_type == FieldType.NUMERIC else f2

        x_field = temp_field
        y_field = num_field

    elif types == {FieldType.NUMERIC}:
        mark = "point"  # scatter
        x_field = f1
        y_field = f2

    else:
        # e.g. cat+cat, temp+temp -> no chart
        return None

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "description": "Agent generated visualization",
        "data": {"values": rows},
        "mark": mark,
        "encoding": {
            "x": {"field": x_field.name, "type": x_field.field_type.value},
            "y": {"field": y_field.name, "type": y_field.field_type.value},
            "tooltip": [
                {"field": x_field.name, "type": x_field.field_type.value},
                {"field": y_field.name, "type": y_field.field_type.value},
            ],
        },
    }

    return spec
