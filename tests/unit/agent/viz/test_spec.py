import unittest

from agent.viz.spec import FieldType, build_vega_lite_spec, infer_fields


class TestVegaLiteSpecBuilder(unittest.TestCase):
    """Unit tests for Vega-Lite spec builder."""

    def test_empty_rows(self):
        """Test with empty or None input."""
        self.assertIsNone(build_vega_lite_spec([]))
        self.assertIsNone(build_vega_lite_spec(None))

    def test_invalid_rows(self):
        """Test with invalid input shapes."""
        # MCP error shape might be a dict, not a list of dicts
        self.assertIsNone(build_vega_lite_spec({"error": "msg"}))
        # List of strings?
        self.assertIsNone(build_vega_lite_spec(["a", "b"]))

    def test_bar_chart_cat_num(self):
        """Test bar chart generation for Categorical + Numeric."""
        data = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
        ]
        spec = build_vega_lite_spec(data)
        self.assertIsNotNone(spec)
        self.assertEqual(spec["mark"], "bar")
        self.assertEqual(spec["encoding"]["x"]["type"], "nominal")
        self.assertEqual(spec["encoding"]["y"]["type"], "quantitative")

    def test_bar_chart_num_cat(self):
        """Test bar chart generation for Numeric + Categorical (flipped)."""
        # Order shouldn't matter for type detection, usually X is cat
        data = [
            {"value": 10, "category": "A"},
            {"value": 20, "category": "B"},
        ]
        spec = build_vega_lite_spec(data)
        self.assertEqual(spec["mark"], "bar")
        # In current implementation, first found is X in loop?
        # Actually logic is: if Cat and Num: mark=bar.
        # x_field = cat, y_field = num.
        # So X should always be categorical.
        self.assertEqual(spec["encoding"]["x"]["field"], "category")
        self.assertEqual(spec["encoding"]["y"]["field"], "value")

    def test_line_chart_temp_num(self):
        """Test line chart generation for Temporal + Numeric."""
        data = [
            {"date": "2023-01-01", "sales": 100},
            {"date": "2023-01-02", "sales": 110},
        ]
        spec = build_vega_lite_spec(data)
        self.assertEqual(spec["mark"], "line")
        # Logic: X = temporal
        self.assertEqual(spec["encoding"]["x"]["type"], "temporal")
        self.assertEqual(spec["encoding"]["y"]["type"], "quantitative")

    def test_scatter_chart_num_num(self):
        """Test scatter chart generation for Numeric + Numeric."""
        data = [
            {"height": 1.5, "weight": 50},
            {"height": 1.6, "weight": 60},
        ]
        spec = build_vega_lite_spec(data)
        self.assertEqual(spec["mark"], "point")
        self.assertEqual(spec["encoding"]["x"]["field"], "height")
        self.assertEqual(spec["encoding"]["y"]["field"], "weight")

    def test_unsupported_shapes(self):
        """Test unsupported data shapes (3 cols, 1 col, etc.)."""
        # 3 columns
        data = [{"a": 1, "b": 2, "c": 3}]
        self.assertIsNone(build_vega_lite_spec(data))

        # 1 column
        data = [{"a": 1}]
        self.assertIsNone(build_vega_lite_spec(data))

        # 2 categorical
        data = [{"a": "A", "b": "B"}]
        self.assertIsNone(build_vega_lite_spec(data))

    def test_inference_mixed(self):
        """Test type inference fallback for mixed types."""
        # Mixed numeric and strings in one column -> Categorical
        data = [
            {"val": 10},
            {"val": "string"},
        ]
        fields = infer_fields(data)
        self.assertEqual(fields[0].field_type, FieldType.CATEGORICAL)

    def test_inference_numeric_strings(self):
        """Test type inference for numeric strings (treated as categorical)."""
        # "10.5" is a string in JSON, so it's categorical/temporal check.
        # It's NOT numeric in Python `isinstance(x, (int, float))` sense.
        # If SQL returns decimals as strings, we treat as categorical currently.
        # (This is a limitation noted in design, valid for MVP).
        data = [{"val": "10.5"}]
        fields = infer_fields(data)
        self.assertEqual(fields[0].field_type, FieldType.CATEGORICAL)
