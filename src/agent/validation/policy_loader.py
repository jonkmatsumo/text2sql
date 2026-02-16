"""Policy loader for runtime enforcement.

Loads and caches row-level security policies from the control-plane database.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional

from opentelemetry import metrics

from common.config.env import get_env_int
from common.lib.otel import get_tracer
from mcp_server.config.control_plane import ControlPlaneDatabase

logger = logging.getLogger(__name__)

meter = metrics.get_meter(__name__)
refresh_duration_histogram = meter.create_histogram(
    name="policy_loader.refresh_duration",
    description="Duration of policy refresh operations in milliseconds",
    unit="ms",
)


@dataclass
class PolicyDefinition:
    """Definition of a row-level security policy for a table."""

    table_name: str
    tenant_column: str
    expression_template: str  # e.g., "{column} = :tenant_id"


class PolicyLoader:
    """Loads and caches policies from the database."""

    _instance: Optional["PolicyLoader"] = None

    def __init__(self) -> None:
        """Initialize the policy loader instance."""
        self._policies: Dict[str, PolicyDefinition] = {}
        self._last_load_time: float = 0.0
        self._CACHE_TTL = 300.0  # 5 minutes
        self._lock = asyncio.Lock()
        self._tracer = get_tracer(__name__)

    @classmethod
    def get_instance(cls) -> "PolicyLoader":
        """Get or create the singleton loader instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Clear the singleton instance (primarily for tests)."""
        cls._instance = None

    async def get_policies(self) -> Dict[str, PolicyDefinition]:
        """Get all active policies, reloading from DB if cache expired.

        Returns:
            Dict mapping table_name to PolicyDefinition.
        """
        now = time.time()
        is_expired = (now - self._last_load_time) > self._CACHE_TTL
        if not self._policies or is_expired:
            async with self._lock:
                # Double-check inside lock
                now = time.time()
                is_expired = (now - self._last_load_time) > self._CACHE_TTL
                if not self._policies or is_expired:
                    timeout_ms = get_env_int("AGENT_CONTROL_PLANE_TIMEOUT_MS", 1000)
                    with self._tracer.start_as_current_span(
                        "policy.get_policies",
                        attributes={
                            "policy.is_expired": is_expired,
                            "policy.has_cache": bool(self._policies),
                        },
                    ) as span:
                        try:
                            await asyncio.wait_for(
                                self._refresh_policies(), timeout=timeout_ms / 1000.0
                            )
                            span.set_attribute("policy.refresh_success", True)
                        except asyncio.TimeoutError:
                            logger.warning(
                                f"Policy refresh timed out after {timeout_ms}ms. "
                                "Using existing or default policies."
                            )
                            span.set_attribute("policy.refresh_success", False)
                            span.set_attribute("policy.refresh_error", "timeout")
                            if not self._policies:
                                self._policies = self._get_default_policies()
                        except Exception as e:
                            logger.error(f"Policy refresh failed: {e}")
                            span.set_attribute("policy.refresh_success", False)
                            span.set_attribute("policy.refresh_error", str(e))
                            if not self._policies:
                                self._policies = self._get_default_policies()

        return self._policies

    async def _refresh_policies(self) -> None:
        """Reload policies from the control-plane database."""
        start_time = time.perf_counter()
        status = "unknown"

        query = """
            SELECT table_name, tenant_column, policy_expression
            FROM row_policies
            WHERE is_enabled = TRUE
        """

        try:
            with self._tracer.start_as_current_span("policy.refresh_policies") as span:
                try:
                    if not ControlPlaneDatabase.is_enabled():
                        # Fallback to hardcoded defaults
                        self._policies = self._get_default_policies()
                        self._last_load_time = time.time()
                        span.set_attribute("policy.source", "defaults")
                        status = "disabled"
                        return

                    if not ControlPlaneDatabase._pool:
                        # Try to init if not already (might happen if agent runs separately)
                        try:
                            await ControlPlaneDatabase.init()
                        except Exception as e:
                            # If generic init fails (e.g. env vars missing), fall back to defaults
                            logger.warning(
                                "Could not initialize ControlPlaneDatabase, "
                                "using default policies."
                            )
                            self._policies = self._get_default_policies()
                            self._last_load_time = time.time()
                            span.set_attribute("policy.source", "defaults_init_fail")
                            span.record_exception(e)
                            status = "init_failed"
                            return

                    async with ControlPlaneDatabase.get_connection() as conn:
                        rows = await conn.fetch(query)

                    new_policies = {}
                    for row in rows:
                        definition = PolicyDefinition(
                            table_name=row["table_name"],
                            tenant_column=row["tenant_column"],
                            expression_template=row["policy_expression"],
                        )
                        new_policies[definition.table_name] = definition

                    self._policies = new_policies
                    self._last_load_time = time.time()
                    logger.info(f"Loaded {len(self._policies)} row policies from control-plane.")
                    span.set_attribute("policy.source", "database")
                    span.set_attribute("policy.count", len(new_policies))
                    status = "success"

                except Exception as e:
                    logger.error(f"Failed to load row policies: {e}")
                    span.record_exception(e)
                    # retain existing policies if refresh fails
                    if not self._policies:
                        self._policies = self._get_default_policies()
                    status = "error"
                    raise e
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            refresh_duration_histogram.record(duration_ms, {"status": status})

    @staticmethod
    def _get_default_policies() -> Dict[str, PolicyDefinition]:
        """Return hardcoded defaults (mirroring legacy RLS)."""
        defaults = [
            ("customer", "store_id", "{column} = :tenant_id"),
            ("rental", "store_id", "{column} = :tenant_id"),
            ("payment", "store_id", "{column} = :tenant_id"),
            ("staff", "store_id", "{column} = :tenant_id"),
            ("inventory", "store_id", "{column} = :tenant_id"),
        ]
        return {table: PolicyDefinition(table, col, expr) for table, col, expr in defaults}
