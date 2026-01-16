import unittest


class TestVizPlaceholder(unittest.TestCase):
    """Placeholder test suite for Phase 0."""

    def test_viz_feature_presence(self):
        """Fail until visualization feature is actually implemented."""
        # This test is a Phase 0 guardrail.
        # It asserts that the feature DOES NOT exist yet, to prove we are starting from zero.
        try:
            import agent_core.viz.spec  # noqa: F401
        except ImportError:
            pass

        # Let's force a failure to strictly acknowledge "Action Required" for next phase.
        self.fail("Visualization feature is not yet implemented (Phase 0 placeholder)")
