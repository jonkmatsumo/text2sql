"""Tests for ExceptionGroup unwrapping utility."""

# ExceptionGroup and BaseExceptionGroup are Python 3.11+ built-ins
# flake8: noqa: F821

from streamlit_app.utils.exceptions import is_single_exception_group, unwrap_single_exception_group


class TestIsSingleExceptionGroup:
    """Tests for is_single_exception_group helper."""

    def test_single_exception_group_returns_true(self):
        """Single-exception case should return True."""
        group = ExceptionGroup("TaskGroup", [ValueError("root cause")])
        assert is_single_exception_group(group) is True

    def test_multi_exception_group_returns_false(self):
        """Multi-exception case should return False."""
        group = ExceptionGroup("TaskGroup", [ValueError("a"), RuntimeError("b")])
        assert is_single_exception_group(group) is False

    def test_regular_exception_returns_false(self):
        """Regular exception should return False."""
        assert is_single_exception_group(ValueError("regular")) is False

    def test_nested_single_group_returns_true(self):
        """Nested single-exception group should return True for outer."""
        inner = ExceptionGroup("inner", [RuntimeError("x")])
        outer = ExceptionGroup("outer", [inner])
        assert is_single_exception_group(outer) is True


class TestUnwrapSingleExceptionGroup:
    """Tests for unwrap_single_exception_group function."""

    def test_unwrap_single_exception_group(self):
        """Single sub-exception case unwraps to the inner exception."""
        inner = ValueError("root cause")
        group = ExceptionGroup("TaskGroup", [inner])

        result = unwrap_single_exception_group(group)

        assert result is inner
        assert str(result) == "root cause"

    def test_unwrap_nested_single_groups(self):
        """Nested single-exception groups recursively unwrap."""
        innermost = RuntimeError("x")
        inner = ExceptionGroup("inner", [innermost])
        outer = ExceptionGroup("outer", [inner])

        result = unwrap_single_exception_group(outer)

        assert result is innermost
        assert str(result) == "x"

    def test_multi_exception_group_unchanged(self):
        """Multi-exception group is returned unchanged."""
        group = ExceptionGroup("TaskGroup", [ValueError("a"), RuntimeError("b")])

        result = unwrap_single_exception_group(group)

        assert result is group
        assert len(result.exceptions) == 2

    def test_regular_exception_unchanged(self):
        """Regular exception is returned unchanged."""
        exc = ValueError("regular error")

        result = unwrap_single_exception_group(exc)

        assert result is exc

    def test_deeply_nested_single_groups(self):
        """Triple-nested single groups unwrap to innermost."""
        deepest = TypeError("deep")
        level2 = ExceptionGroup("level2", [deepest])
        level1 = ExceptionGroup("level1", [level2])
        level0 = ExceptionGroup("level0", [level1])

        result = unwrap_single_exception_group(level0)

        assert result is deepest

    def test_mixed_nesting_stops_at_multi(self):
        """Unwrapping stops when encountering multi-exception group."""
        inner = ExceptionGroup("inner", [ValueError("a"), RuntimeError("b")])
        outer = ExceptionGroup("outer", [inner])

        result = unwrap_single_exception_group(outer)

        # Unwraps outer (single) but stops at inner (multi)
        assert result is inner
        assert len(result.exceptions) == 2

    def test_base_exception_group_support(self):
        """Handle BaseExceptionGroup for BaseException subclasses."""
        inner = KeyboardInterrupt()
        group = BaseExceptionGroup("interrupted", [inner])

        result = unwrap_single_exception_group(group)

        assert result is inner
