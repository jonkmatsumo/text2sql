"""Telemetry service for abstracting tracing and observability.

This module provides a unified interface for tracing, metrics, and metadata
logging, allowing the agent to be agnostic of the underlying backend (e.g., MLflow, OTEL).
"""

import abc
import contextlib
import json
import logging
import os
from enum import Enum
from typing import Any, Dict, Optional

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

logger = logging.getLogger(__name__)


class SpanType(Enum):
    """Semantic span types mapping to MLflow/OTEL concepts."""

    CHAIN = "CHAIN"
    TOOL = "TOOL"
    RETRIEVER = "RETRIEVER"
    CHAT_MODEL = "CHAT_MODEL"
    PARSER = "PARSER"
    UNKNOWN = "UNKNOWN"


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


class MlflowTelemetrySpan(TelemetrySpan):
    """MLflow implementation of TelemetrySpan."""

    def __init__(self, mlflow_span):
        """Initialize with an MLflow span object."""
        self._span = mlflow_span

    def set_inputs(self, inputs: Dict[str, Any]) -> None:
        """Set span inputs."""
        self._span.set_inputs(inputs)

    def set_outputs(self, outputs: Dict[str, Any]) -> None:
        """Set span outputs."""
        self._span.set_outputs(outputs)

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a single span attribute."""
        self._span.set_attribute(key, value)

    def set_attributes(self, attributes: Dict[str, Any]) -> None:
        """Set multiple span attributes."""
        for k, v in attributes.items():
            self._span.set_attribute(k, v)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add a timed event to the span."""
        self._span.add_event(name, attributes)


class MlflowTelemetryBackend(TelemetryBackend):
    """MLflow implementation of TelemetryBackend."""

    def __init__(self):
        """Initialize both mlflow object and span type map."""
        self._mlflow = None
        self._span_type_map = {}

    def _ensure_mlflow(self):
        if self._mlflow is None:
            import mlflow

            self._mlflow = mlflow
            self._span_type_map = {
                SpanType.CHAIN: mlflow.entities.SpanType.CHAIN,
                SpanType.TOOL: mlflow.entities.SpanType.TOOL,
                SpanType.RETRIEVER: mlflow.entities.SpanType.RETRIEVER,
                SpanType.CHAT_MODEL: mlflow.entities.SpanType.CHAT_MODEL,
                SpanType.PARSER: mlflow.entities.SpanType.PARSER,
                SpanType.UNKNOWN: mlflow.entities.SpanType.UNKNOWN,
            }
        return self._mlflow

    def configure(self, tracking_uri: Optional[str] = None, autolog: bool = True, **kwargs) -> None:
        """Configure MLflow tracking and autologging."""
        mlflow = self._ensure_mlflow()
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        if autolog:
            import mlflow.langchain

            mlflow.langchain.autolog(**kwargs)

    @contextlib.contextmanager
    def start_span(
        self,
        name: str,
        span_type: SpanType = SpanType.UNKNOWN,
        inputs: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Start an MLflow span as a context manager."""
        mlflow = self._ensure_mlflow()
        ml_span_type = self._span_type_map.get(span_type, mlflow.entities.SpanType.UNKNOWN)

        with mlflow.start_span(name=name, span_type=ml_span_type) as ml_span:
            span = MlflowTelemetrySpan(ml_span)
            if inputs:
                span.set_inputs(inputs)
            if attributes:
                span.set_attributes(attributes)
            yield span

    def update_current_trace(self, metadata: Dict[str, Any]) -> None:
        """Update the current active trace with metadata."""
        mlflow = self._ensure_mlflow()
        try:
            mlflow.update_current_trace(metadata=metadata)
        except Exception:
            # Often fails if no active trace, mirroring current graph.py behavior
            pass


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
        """Configure OTEL.

        Note: Actual SDK configuration (exporters, etc.) is usually handled
        externally or via env vars, but we provide a hook here.
        """
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
        base_attrs = {"span.type": span_type.value}
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


class DualTelemetrySpan(TelemetrySpan):
    """Composite span that forwards calls to two backends."""

    def __init__(self, primary: TelemetrySpan, secondary: Optional[TelemetrySpan]):
        """Initialize with primary and optional secondary span."""
        self.primary = primary
        self.secondary = secondary

    def set_inputs(self, inputs: Dict[str, Any]) -> None:
        """Set inputs on both spans, secondary is best-effort."""
        self.primary.set_inputs(inputs)
        if self.secondary:
            try:
                self.secondary.set_inputs(inputs)
            except Exception:
                logger.warning("Secondary backend set_inputs failed", exc_info=True)

    def set_outputs(self, outputs: Dict[str, Any]) -> None:
        """Set outputs on both spans, secondary is best-effort."""
        self.primary.set_outputs(outputs)
        if self.secondary:
            try:
                self.secondary.set_outputs(outputs)
            except Exception:
                logger.warning("Secondary backend set_outputs failed", exc_info=True)

    def set_attribute(self, key: str, value: Any) -> None:
        """Set attribute on both spans, secondary is best-effort."""
        self.primary.set_attribute(key, value)
        if self.secondary:
            try:
                self.secondary.set_attribute(key, value)
            except Exception:
                logger.warning("Secondary backend set_attribute failed", exc_info=True)

    def set_attributes(self, attributes: Dict[str, Any]) -> None:
        """Set attributes on both spans, secondary is best-effort."""
        self.primary.set_attributes(attributes)
        if self.secondary:
            try:
                self.secondary.set_attributes(attributes)
            except Exception:
                logger.warning("Secondary backend set_attributes failed", exc_info=True)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add event to both spans, secondary is best-effort."""
        self.primary.add_event(name, attributes)
        if self.secondary:
            try:
                self.secondary.add_event(name, attributes)
            except Exception:
                logger.warning("Secondary backend add_event failed", exc_info=True)


class DualTelemetryBackend(TelemetryBackend):
    """Composite backend that writes to two backends simultaneously."""

    def __init__(self, primary: TelemetryBackend, secondary: TelemetryBackend):
        """Initialize with primary and secondary backends."""
        self.primary = primary
        self.secondary = secondary

    def configure(self, **kwargs) -> None:
        """Configure both backends, secondary is best-effort."""
        self.primary.configure(**kwargs)
        try:
            self.secondary.configure(**kwargs)
        except Exception:
            logger.warning("Secondary backend configure failed", exc_info=True)

    @contextlib.contextmanager
    def start_span(
        self,
        name: str,
        span_type: SpanType = SpanType.UNKNOWN,
        inputs: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Start span on both backends using ExitStack for safe nesting."""
        with contextlib.ExitStack() as stack:
            # Primary is strict, errors here bubble up
            p_cm = self.primary.start_span(name, span_type, inputs, attributes)
            p_span = stack.enter_context(p_cm)

            # Secondary is best-effort
            s_span = None
            try:
                s_cm = self.secondary.start_span(name, span_type, inputs, attributes)
                s_span = stack.enter_context(s_cm)
            except Exception:
                logger.warning("Secondary backend start_span failed", exc_info=True)

            yield DualTelemetrySpan(p_span, s_span)

    def update_current_trace(self, metadata: Dict[str, Any]) -> None:
        """Update current trace on both backends, secondary is best-effort."""
        self.primary.update_current_trace(metadata)
        try:
            self.secondary.update_current_trace(metadata)
        except Exception:
            logger.warning("Secondary backend update_current_trace failed", exc_info=True)


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


class TelemetryService:
    """Public surface for telemetry calls."""

    def __init__(self, backend: Optional[TelemetryBackend] = None):
        """Initialize the telemetry service.

        Args:
            backend: The telemetry backend to use. If not provided,
                    defaults to TELEMETRY_BACKEND env var.
        """
        if backend:
            self._backend = backend
        else:
            backend_type = os.getenv("TELEMETRY_BACKEND", "mlflow").lower()
            if backend_type == "otel":
                self._backend = OTELTelemetryBackend()
            elif backend_type == "dual":
                self._backend = DualTelemetryBackend(
                    primary=MlflowTelemetryBackend(), secondary=OTELTelemetryBackend()
                )
            else:
                self._backend = MlflowTelemetryBackend()

    def set_backend(self, backend: TelemetryBackend) -> None:
        """Switch backend at runtime (useful for testing)."""
        self._backend = backend

    def configure(self, tracking_uri: Optional[str] = None, autolog: bool = True, **kwargs) -> None:
        """Configure telemetry settings.

        Args:
            tracking_uri: MLflow tracking URI.
            autolog: Whether to enable LangChain autologging.
            **kwargs: Additional arguments passed to autologging (e.g., run_tracer_inline=True).
        """
        self._backend.configure(tracking_uri=tracking_uri, autolog=autolog, **kwargs)

    def start_span(
        self,
        name: str,
        span_type: SpanType = SpanType.UNKNOWN,
        inputs: Optional[Dict[str, Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ):
        """Start a new span."""
        return self._backend.start_span(
            name=name,
            span_type=span_type,
            inputs=inputs,
            attributes=attributes,
        )

    def update_current_trace(self, metadata: Dict[str, Any]) -> None:
        """Update current trace with metadata."""
        self._backend.update_current_trace(metadata)


# Global instance for easy access
telemetry = TelemetryService()
