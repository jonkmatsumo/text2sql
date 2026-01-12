import asyncio
import unittest
from unittest.mock import patch

from agent_core.telemetry import OTELTelemetryBackend, telemetry
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


class TestNodePropagation(unittest.IsolatedAsyncioTestCase):
    """Test suite for verifying node context propagation."""

    async def asyncSetUp(self):
        """Set up test environment with OTEL backend."""
        self.exporter = InMemorySpanExporter()
        self.provider = TracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer("test")

        self.backend = OTELTelemetryBackend()
        telemetry.set_backend(self.backend)
        self.service = telemetry  # Use the global one for clarity

    async def test_node_parenting_failure_simulation(self):
        """Simulate how nodes might be called without explicit context propagation."""

        async def mock_node(state):
            with telemetry.start_span("node_span"):
                return {"done": True}

        # 1. Start 'root' span manually (simulating LangGraph autolog)
        with patch("opentelemetry.trace.get_tracer", return_value=self.tracer):
            with self.service.start_span("root"):
                # Simulating calling a node in a way that might lose context
                # (e.g. if LangGraph scheduled it in a way that cleared contextvars)
                # Here we just call it directly which SHOULD work if it's the same task.
                # To simulate FAILURE, we can run it in a separate task without propagation.
                task = asyncio.create_task(mock_node({}))
                await task

        spans = self.exporter.get_finished_spans()
        self.assertEqual(len(spans), 2)

        root = next(s for s in spans if s.name == "root")
        node = next(s for s in spans if s.name == "node_span")

        # In a standard asyncio.create_task, contextvars ARE propagated.
        # If the user says it's broken, maybe LangGraph or some inner part of the agent
        # is using something that clears it, or they are using threads.

        print(f"Root Trace ID: {root.context.trace_id}")
        print(f"Node Trace ID: {node.context.trace_id}")
        print(f"Node Parent ID: {node.parent.span_id if node.parent else 'None'}")

    async def test_node_parenting_with_explicit_propagation(self):
        """Verify that explicit use_context fixes parenting even across task boundaries."""

        async def mock_node(state, ctx):
            with telemetry.use_context(ctx):
                with telemetry.start_span("node_span"):
                    return {"done": True}

        with patch("opentelemetry.trace.get_tracer", return_value=self.tracer):
            with self.service.start_span("root"):
                ctx = telemetry.capture_context()
                # Even if this was a thread or a cleared context, passing ctx manually helps
                task = asyncio.create_task(mock_node({}, ctx))
                await task

        spans = self.exporter.get_finished_spans()
        root = next(s for s in spans if s.name == "root")
        node = next(s for s in spans if s.name == "node_span")

        self.assertEqual(node.context.trace_id, root.context.trace_id)
        self.assertEqual(node.parent.span_id, root.context.span_id)

    async def test_node_tool_parenting(self):
        """Verify that a tool span started inside a wrapped node is correctly parented."""

        async def mock_tool_call():
            # Simulating LangChain tool span
            with self.tracer.start_as_current_span("tool_span"):
                return "ok"

        async def mock_node(state):
            # The node start_span is manual
            with telemetry.start_span("node_span"):
                await mock_tool_call()
                return {"done": True}

        def node_wrapper(node_func):
            async def wrapped(state):
                ctx = state.get("telemetry_context")
                with telemetry.use_context(ctx):
                    return await node_func(state)

            return wrapped

        wrapped_node = node_wrapper(mock_node)

        with patch("opentelemetry.trace.get_tracer", return_value=self.tracer):
            # 1. Start application root span
            with self.service.start_span("app_root"):
                ctx = telemetry.capture_context()
                state = {"telemetry_context": ctx}

                # 2. Call wrapped node (simulating LangGraph)
                # We simulate a potential async boundary here
                await asyncio.create_task(wrapped_node(state))

        spans = self.exporter.get_finished_spans()
        # Finished order: tool_span, node_span, app_root
        app_root = next(s for s in spans if s.name == "app_root")
        node_span = next(s for s in spans if s.name == "node_span")
        tool_span = next(s for s in spans if s.name == "tool_span")

        # Assert node_span's parent is app_root
        self.assertEqual(node_span.parent.span_id, app_root.context.span_id)
        self.assertEqual(node_span.context.trace_id, app_root.context.trace_id)

        # Assert tool_span's parent is node_span
        self.assertEqual(tool_span.parent.span_id, node_span.context.span_id)
        self.assertEqual(tool_span.context.trace_id, app_root.context.trace_id)


if __name__ == "__main__":
    unittest.main()
