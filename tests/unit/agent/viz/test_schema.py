import unittest

from agent.viz.schema import FieldType, build_chart_schema, infer_fields


class TestChartSchemaBuilder(unittest.TestCase):
    """Unit tests for ChartSchema builder."""

    def test_empty_rows(self):
        """Return None for empty input."""
        self.assertIsNone(build_chart_schema([]))
        self.assertIsNone(build_chart_schema(None))

    def test_invalid_rows(self):
        """Return None for invalid input shapes."""
        self.assertIsNone(build_chart_schema({"error": "msg"}))
        self.assertIsNone(build_chart_schema(["a", "b"]))

    def test_bar_chart_cat_num(self):
        """Create bar schema for categorical + numeric data."""
        data = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
        ]
        schema = build_chart_schema(data)
        self.assertIsNotNone(schema)
        self.assertEqual(schema["chartType"], "bar")
        self.assertEqual(schema["xAxis"]["label"], "category")
        self.assertEqual(schema["yAxis"]["label"], "value")

    def test_line_chart_temp_num(self):
        """Create line schema for temporal + numeric data."""
        data = [
            {"date": "2023-01-01", "sales": 100},
            {"date": "2023-01-02", "sales": 110},
        ]
        schema = build_chart_schema(data)
        self.assertEqual(schema["chartType"], "line")
        self.assertEqual(schema["xAxis"]["label"], "date")
        self.assertEqual(schema["xAxis"]["format"], "%m/%d %H:%M")

    def test_scatter_chart_num_num(self):
        """Create scatter schema for numeric + numeric data."""
        data = [
            {"height": 1.5, "weight": 50},
            {"height": 1.6, "weight": 60},
        ]
        schema = build_chart_schema(data)
        self.assertEqual(schema["chartType"], "scatter")
        self.assertEqual(schema["xAxis"]["label"], "height")
        self.assertEqual(schema["yAxis"]["label"], "weight")

    def test_unsupported_shapes(self):
        """Return None for unsupported shapes."""
        data = [{"a": 1, "b": 2, "c": 3}]
        self.assertIsNone(build_chart_schema(data))

        data = [{"a": 1}]
        self.assertIsNone(build_chart_schema(data))

        data = [{"a": "A", "b": "B"}]
        self.assertIsNone(build_chart_schema(data))

    def test_inference_mixed(self):
        """Treat mixed types as categorical."""
        data = [
            {"val": 10},
            {"val": "string"},
        ]
        fields = infer_fields(data)
        self.assertEqual(fields[0].field_type, FieldType.CATEGORICAL)

    def test_inference_numeric_strings(self):
        """Treat numeric-like strings as categorical."""
        data = [{"val": "10.5"}]
        fields = infer_fields(data)
        self.assertEqual(fields[0].field_type, FieldType.CATEGORICAL)
