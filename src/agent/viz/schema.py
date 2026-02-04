import re
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


class FieldType(Enum):
    """Enumeration of field types used for chart schema inference."""

    NUMERIC = "numeric"
    TEMPORAL = "temporal"
    CATEGORICAL = "categorical"


@dataclass
class FieldSpec:
    """Specification for a single field in the dataset."""

    name: str
    field_type: FieldType


@dataclass
class Point:
    """Chart data point."""

    x: Any
    y: Optional[float]


@dataclass
class Series:
    """Series of chart points."""

    name: str
    points: List[Point]


@dataclass
class AxisSpec:
    """Axis configuration for a chart."""

    label: Optional[str] = None
    format: Optional[str] = None


@dataclass
class ChartSchema:
    """Chart schema payload for the UI renderer."""

    chartType: str
    series: List[Series]
    xAxis: Optional[AxisSpec] = None
    yAxis: Optional[AxisSpec] = None


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_temporal(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    iso_date_pattern = r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?)?Z?$"
    return bool(re.match(iso_date_pattern, value))


def infer_fields(rows: List[Dict[str, Any]], sample_size: int = 100) -> List[FieldSpec]:
    """Infer field names and types from a list of row dictionaries."""
    if not rows:
        return []

    keys = list(rows[0].keys())
    specs = []

    for key in keys:
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
            field_type = FieldType.CATEGORICAL
        elif is_num:
            field_type = FieldType.NUMERIC
        elif is_temp:
            field_type = FieldType.TEMPORAL
        else:
            field_type = FieldType.CATEGORICAL

        specs.append(FieldSpec(name=key, field_type=field_type))

    return specs


def _drop_none(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {key: _drop_none(value) for key, value in payload.items() if value is not None}
    if isinstance(payload, list):
        return [_drop_none(item) for item in payload]
    return payload


def build_chart_schema(
    rows: List[Dict[str, Any]], chart_hint: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Build a ChartSchema payload from data rows."""
    if not rows or not isinstance(rows, list):
        return None

    if not isinstance(rows[0], dict):
        return None

    fields = infer_fields(rows)

    if len(fields) != 2:
        return None

    f1, f2 = fields[0], fields[1]

    chart_type = None
    x_field = None
    y_field = None

    types = {f1.field_type, f2.field_type}

    if FieldType.CATEGORICAL in types and FieldType.NUMERIC in types:
        chart_type = "bar"
        cat_field = f1 if f1.field_type == FieldType.CATEGORICAL else f2
        num_field = f1 if f1.field_type == FieldType.NUMERIC else f2
        x_field = cat_field
        y_field = num_field
    elif FieldType.TEMPORAL in types and FieldType.NUMERIC in types:
        chart_type = "line"
        temp_field = f1 if f1.field_type == FieldType.TEMPORAL else f2
        num_field = f1 if f1.field_type == FieldType.NUMERIC else f2
        x_field = temp_field
        y_field = num_field
    elif types == {FieldType.NUMERIC}:
        chart_type = "scatter"
        x_field = f1
        y_field = f2
    else:
        return None

    points = [Point(x=row.get(x_field.name), y=row.get(y_field.name)) for row in rows]

    x_axis = AxisSpec(label=x_field.name)
    if x_field.field_type == FieldType.TEMPORAL:
        x_axis.format = "%m/%d %H:%M"

    schema = ChartSchema(
        chartType=chart_type,
        series=[Series(name=y_field.name, points=points)],
        xAxis=x_axis,
        yAxis=AxisSpec(label=y_field.name),
    )

    _ = chart_hint
    return _drop_none(asdict(schema))
