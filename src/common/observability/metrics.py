"""Optional low-cardinality metrics helpers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from opentelemetry import metrics

from common.config.env import get_env_bool

logger = logging.getLogger(__name__)


def is_otel_exporter_configured() -> bool:
    """Return True when OTEL exporter environment indicates external export is configured."""
    if get_env_bool("OTEL_DISABLE_EXPORTER", False):
        return False

    endpoint = (os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or "").strip()
    metrics_endpoint = (os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") or "").strip()
    metrics_exporter = (os.getenv("OTEL_METRICS_EXPORTER") or "").strip().lower()
    if metrics_exporter == "none":
        return False

    return bool(endpoint or metrics_endpoint)


def is_metrics_enabled(enabled_env_var: str) -> bool:
    """Resolve metric enablement with explicit override support."""
    raw = os.getenv(enabled_env_var)
    if raw is not None:
        try:
            return get_env_bool(enabled_env_var, False) is True
        except ValueError:
            logger.warning("Invalid %s value '%s'; metrics disabled.", enabled_env_var, raw)
            return False
    return is_otel_exporter_configured()


def _normalize_attributes(attributes: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in (attributes or {}).items():
        if value is None:
            continue
        if isinstance(value, bool):
            normalized[key] = "true" if value else "false"
            continue
        if isinstance(value, (str, int, float)):
            normalized[key] = value
            continue
        normalized[key] = str(value)
    return normalized


@dataclass
class OptionalMetrics:
    """Thin wrapper around OTEL metrics with env-based enablement."""

    meter_name: str
    enabled_env_var: str
    _meter: Any = None
    _counters: dict[str, Any] = field(default_factory=dict)
    _histograms: dict[str, Any] = field(default_factory=dict)

    def _enabled(self) -> bool:
        return is_metrics_enabled(self.enabled_env_var)

    def _get_meter(self):
        if self._meter is None:
            self._meter = metrics.get_meter(self.meter_name)
        return self._meter

    def add_counter(
        self,
        name: str,
        value: int = 1,
        *,
        description: str = "",
        unit: str = "1",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add to a monotonic counter when metrics are enabled."""
        if not self._enabled():
            return
        try:
            counter = self._counters.get(name)
            if counter is None:
                counter = self._get_meter().create_counter(
                    name=name,
                    description=description,
                    unit=unit,
                )
                self._counters[name] = counter
            counter.add(int(value), _normalize_attributes(attributes))
        except Exception as exc:
            logger.debug("Counter metric emission failed for %s: %s", name, exc)

    def record_histogram(
        self,
        name: str,
        value: float,
        *,
        description: str = "",
        unit: str = "1",
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record a histogram datapoint when metrics are enabled."""
        if not self._enabled():
            return
        try:
            histogram = self._histograms.get(name)
            if histogram is None:
                histogram = self._get_meter().create_histogram(
                    name=name,
                    description=description,
                    unit=unit,
                )
                self._histograms[name] = histogram
            histogram.record(float(value), _normalize_attributes(attributes))
        except Exception as exc:
            logger.debug("Histogram metric emission failed for %s: %s", name, exc)


agent_metrics = OptionalMetrics(
    meter_name="text2sql-agent",
    enabled_env_var="AGENT_OBSERVABILITY_METRICS_ENABLED",
)

mcp_metrics = OptionalMetrics(
    meter_name="text2sql-mcp",
    enabled_env_var="MCP_OBSERVABILITY_METRICS_ENABLED",
)
