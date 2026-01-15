"""Telemetry service for abstracting tracing and observability.

This module provides a unified interface for tracing, metrics, and metadata
logging, allowing the agent to be agnostic of the underlying backend (e.g., MLflow, OTEL).
"""

import abc
import contextlib
import json
import logging
import os
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional

from opentelemetry import context, propagate, trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

from common.config.env import get_env_str

logger = logging.getLogger(__name__)

OTEL_EXPORTER_OTLP_ENDPOINT = get_env_str("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
OTEL_EXPORTER_OTLP_PROTOCOL = get_env_str("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
OTEL_SERVICE_NAME = get_env_str("OTEL_SERVICE_NAME", "text2sql-agent")

_otel_initialized = False

# "Sticky" metadata that persists across spans in the same execution context
_sticky_metadata: ContextVar[Dict[str, Any]] = ContextVar("sticky_metadata", default={})


def _setup_otel_sdk():
    """Configure the OTEL SDK once."""
    global _otel_initialized
    if _otel_initialized:
        return

    resource = Resource.create({SERVICE_NAME: OTEL_SERVICE_NAME})
    provider = TracerProvider(resource=resource)

    # Check if we are running in a test environment
    if (
        "PYTEST_CURRENT_TEST" in os.environ
        and os.environ.get("OTEL_ENABLE_IN_TESTS", "").lower() != "true"
    ):
        # In tests, specifically avoid OTLP to prevent connection retry noise
        # unless explicitly enabled.
        trace.set_tracer_provider(provider)
        _otel_initialized = True
        logger.info("OTEL SDK initialized in TEST mode (No-op exporter)")
        return

    if OTEL_EXPORTER_OTLP_PROTOCOL == "grpc":
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        except ImportError:
            # Fallback to HTTP if grpc is not available or mismatch
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    else:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    exporter = OTLPSpanExporter(endpoint=OTEL_EXPORTER_OTLP_ENDPOINT)
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    # Set as global tracer provider
    trace.set_tracer_provider(provider)
    _otel_initialized = True
    logger.info(f"OTEL SDK initialized with endpoint: {OTEL_EXPORTER_OTLP_ENDPOINT}")


class SpanType(Enum):
    """Semantic span types mapping to MLflow/OTEL concepts."""

    CHAIN = "CHAIN"
    TOOL = "TOOL"
    RETRIEVER = "RETRIEVER"
    CHAT_MODEL = "CHAT_MODEL"
    PARSER = "PARSER"
    UNKNOWN = "UNKNOWN"


@dataclass
class TelemetryContext:
    """Opaque container for tracing context."""

    otel_context: Optional[Any] = None
    metadata: Optional[Dict[str, Any]] = None
    sticky_metadata: Optional[Dict[str, Any]] = None


class TelemetrySpan(abc.ABC):
    """Abstract interface for a telemetry span."""

    @abc.abstractmethod
    def set_inputs(self, inputs: Dict[str, Any]) -> None:
        """Set span inputs."""
        pass

    @abc.abstractmethod
    def set_outputs(self, outputs: Dict[str, Any]) -> None:
        """Set span outputs."""
        pass

    @abc.abstractmethod
    def set_attribute(self, key: str, value: Any) -> None:
        """Set a single span attribute."""
        pass

    @abc.abstractmethod
    def set_attributes(self, attributes: Dict[str, Any]) -> None:
        """Set multiple span attributes."""
        pass

    @abc.abstractmethod
    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add a timed event to the span."""
        pass


class TelemetryBackend(abc.ABC):
    """Abstract base class for telemetry backends."""

    @abc.abstractmethod
    def configure(self, **kwargs) -> None:
        """Configure the backend (e.g., tracking URI, autologging)."""
        pass

    @abc.abstractmethod
    @contextlib.contextmanager
    def start_span(
        self,
        name: str,
        span_type: SpanType = SpanType.UNKNOWN,
        inputs: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Start a span as a context manager."""
        yield None

    @abc.abstractmethod
    def update_current_trace(self, metadata: Dict[str, Any]) -> None:
        """Update the current active trace with metadata."""
        pass

    @abc.abstractmethod
    def capture_context(self) -> TelemetryContext:
        """Capture the current tracing context."""
        pass

    @abc.abstractmethod
    @contextlib.contextmanager
    def use_context(self, ctx: TelemetryContext):
        """Use a previously captured context as active."""
        yield


class OTELTelemetrySpan(TelemetrySpan):
    """OpenTelemetry implementation of TelemetrySpan."""

    def __init__(self, otel_span):
        """Initialize with an OTEL span object."""
        self._span = otel_span

    def set_inputs(self, inputs: Dict[str, Any]) -> None:
        """Set span inputs as JSON attribute."""
        self._span.set_attribute("telemetry.inputs_json", json.dumps(inputs))

    def set_outputs(self, outputs: Dict[str, Any]) -> None:
        """Set span outputs as JSON attribute and handle error status."""
        self._span.set_attribute("telemetry.outputs_json", json.dumps(outputs))

        # Check for error in outputs
        error = outputs.get("error")
        if error:
            self._span.set_attribute("telemetry.error", str(error))
            self._span.set_status(Status(StatusCode.ERROR, description=str(error)))

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a single span attribute."""
        self._span.set_attribute(key, value)

    def set_attributes(self, attributes: Dict[str, Any]) -> None:
        """Set multiple span attributes."""
        self._span.set_attributes(attributes)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add a timed event to the span."""
        self._span.add_event(name, attributes or {})


class OTELTelemetryBackend(TelemetryBackend):
    """OpenTelemetry implementation of TelemetryBackend."""

    def __init__(self, tracer_name: str = "text2sql-agent"):
        """Initialize the OTEL backend."""
        self.tracer_name = tracer_name
        self._tracer = None

    def _ensure_tracer(self):
        if self._tracer is None:
            self._tracer = trace.get_tracer(self.tracer_name)
        return self._tracer

    def configure(self, **kwargs) -> None:
        """Configure OTEL SDK and initialize tracer."""
        _setup_otel_sdk()
        # Ensure tracer is initialized
        self._ensure_tracer()

    @contextlib.contextmanager
    def start_span(
        self,
        name: str,
        span_type: SpanType = SpanType.UNKNOWN,
        inputs: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Start an OTEL span as a context manager."""
        tracer = self._ensure_tracer()

        # Map span_type to attribute as OTEL SpanKind is usually INTERNAL for these
        base_attrs = {
            "span.type": span_type.value,
            "service.name": self.tracer_name,
        }
        if attributes:
            base_attrs.update(attributes)

        with tracer.start_as_current_span(
            name=name, kind=trace.SpanKind.INTERNAL, attributes=base_attrs
        ) as otel_span:
            span = OTELTelemetrySpan(otel_span)
            if inputs:
                span.set_inputs(inputs)
            yield span

    def update_current_trace(self, metadata: Dict[str, Any]) -> None:
        """Update the current active span with metadata (best effort)."""
        current_span = trace.get_current_span()
        if current_span.is_recording():
            current_span.set_attributes(metadata)
        else:
            logger.debug("update_current_trace: No recording span in context")

    def capture_context(self) -> TelemetryContext:
        """Capture current OTEL context."""
        return TelemetryContext(otel_context=context.get_current())

    @contextlib.contextmanager
    def use_context(self, ctx: TelemetryContext):
        """Restore active OTEL context."""
        token = None
        if ctx.otel_context:
            token = context.attach(ctx.otel_context)
        try:
            yield
        finally:
            if token:
                context.detach(token)


class InMemoryTelemetrySpan(TelemetrySpan):
    """In-memory implementation of TelemetrySpan for testing."""

    def __init__(self, name: str, span_type: SpanType):
        """Initialize an in-memory span."""
        self.name = name
        self.span_type = span_type
        self.inputs = {}
        self.outputs = {}
        self.attributes = {}
        self.events = []
        self.is_finished = False

    def set_inputs(self, inputs: Dict[str, Any]) -> None:
        """Set span inputs."""
        self.inputs.update(inputs)

    def set_outputs(self, outputs: Dict[str, Any]) -> None:
        """Set span outputs."""
        self.outputs.update(outputs)

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a single span attribute."""
        self.attributes[key] = value

    def set_attributes(self, attributes: Dict[str, Any]) -> None:
        """Set multiple span attributes."""
        self.attributes.update(attributes)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add a timed event to the span."""
        self.events.append({"name": name, "attributes": attributes or {}})


class InMemoryTelemetryBackend(TelemetryBackend):
    """In-memory implementation of TelemetryBackend for testing."""

    def __init__(self):
        """Initialize with empty storage."""
        self.spans = []
        self.trace_metadata = {}
        self.config = {}

    def configure(self, **kwargs) -> None:
        """Configure via kwargs."""
        self.config.update(kwargs)

    @contextlib.contextmanager
    def start_span(
        self,
        name: str,
        span_type: SpanType = SpanType.UNKNOWN,
        inputs: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Start a span as a context manager."""
        span = InMemoryTelemetrySpan(name, span_type)
        if inputs:
            span.set_inputs(inputs)
        if attributes:
            span.set_attributes(attributes)
        self.spans.append(span)
        try:
            yield span
        finally:
            span.is_finished = True

    def update_current_trace(self, metadata: Dict[str, Any]) -> None:
        """Update current trace metadata."""
        self.trace_metadata.update(metadata)

    def capture_context(self) -> TelemetryContext:
        """Capture in-memory context."""
        return TelemetryContext()

    @contextlib.contextmanager
    def use_context(self, ctx: TelemetryContext):
        """Use in-memory context."""
        yield


class TelemetryService:
    """Public surface for telemetry calls."""

    def __init__(self, backend: Optional[TelemetryBackend] = None):
        """Initialize the telemetry service.

        Args:
            backend: The telemetry backend to use. If not provided,
                    defaults to OTELTelemetryBackend.
        """
        if backend:
            self._backend = backend
        else:
            self._backend = OTELTelemetryBackend()

    def set_backend(self, backend: TelemetryBackend) -> None:
        """Switch backend at runtime (useful for testing)."""
        self._backend = backend

    def configure(self, tracking_uri: Optional[str] = None, autolog: bool = True, **kwargs) -> None:
        """Configure telemetry settings.

        Args:
            tracking_uri: Ignored (MLflow legacy).
            autolog: Ignored (MLflow legacy).
            **kwargs: Additional arguments passed to backend configure.
        """
        # OTEL backend doesn't use tracking_uri or autolog params,
        # but we accept them for compatibility
        self._backend.configure(**kwargs)

    @contextlib.contextmanager
    def start_span(
        self,
        name: str,
        span_type: SpanType = SpanType.UNKNOWN,
        inputs: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Start a new span.

        This method acts as a boundary for sticky metadata. Any metadata added
        via update_current_trace inside this span will be inherited by child
        spans but will be discarded when this span exits.
        """
        # Snapshot current sticky metadata to prevent leaks outside this span
        token = _sticky_metadata.set(_sticky_metadata.get().copy())

        try:
            # Merge sticky metadata into attributes for this span
            merged_attributes = _sticky_metadata.get().copy()
            if attributes:
                merged_attributes.update(attributes)

            with self._backend.start_span(
                name=name,
                span_type=span_type,
                inputs=inputs,
                attributes=merged_attributes,
            ) as span:
                yield span
        finally:
            # Restore sticky metadata to pre-span state
            _sticky_metadata.reset(token)

    def update_current_trace(self, metadata: Dict[str, Any]) -> None:
        """Update current trace with metadata and make it sticky."""
        # Update sticky metadata for future spans
        current = _sticky_metadata.get().copy()
        current.update(metadata)
        _sticky_metadata.set(current)

        # Update current span/trace in backend
        self._backend.update_current_trace(metadata)

    def capture_context(self) -> TelemetryContext:
        """Capture current tracing context including sticky metadata."""
        ctx = self._backend.capture_context()
        ctx.sticky_metadata = _sticky_metadata.get().copy()
        return ctx

    def inject_context(self, carrier: Dict[str, str]) -> None:
        """Inject current context into a carrier (e.g. headers)."""
        propagate.inject(carrier)

    @contextlib.contextmanager
    def use_context(self, ctx: TelemetryContext):
        """Use a previously captured context."""
        # Restore sticky metadata
        token = None
        if ctx.sticky_metadata is not None:
            token = _sticky_metadata.set(ctx.sticky_metadata)

        try:
            with self._backend.use_context(ctx):
                yield
        finally:
            if token:
                _sticky_metadata.reset(token)

    def extract_context(self, carrier: Dict[str, str]) -> TelemetryContext:
        """Extract context from a carrier and return it."""
        otel_ctx = propagate.extract(carrier)
        return TelemetryContext(otel_context=otel_ctx)

    def run_with_context(self, ctx: TelemetryContext, func: Callable, *args, **kwargs):
        """Run a function under a specific context."""
        with self.use_context(ctx):
            return func(*args, **kwargs)

    async def arun_with_context(self, ctx: TelemetryContext, func: Callable, *args, **kwargs):
        """Run an async function under a specific context."""
        with self.use_context(ctx):
            return await func(*args, **kwargs)


# Global instance for easy access
telemetry = TelemetryService()
