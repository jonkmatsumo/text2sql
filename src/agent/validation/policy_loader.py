"""Policy loader for runtime enforcement.

Loads and caches row-level security policies from the control-plane database.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional

from common.config.env import get_env_int
from mcp_server.config.control_plane import ControlPlaneDatabase

logger = logging.getLogger(__name__)


@dataclass
class PolicyDefinition:
    """Definition of a row-level security policy for a table."""

    table_name: str
    tenant_column: str
    expression_template: str  # e.g., "{column} = :tenant_id"


class PolicyLoader:
    """Loads and caches policies from the database."""

    _policies: Dict[str, PolicyDefinition] = {}
    _last_load_time: float = 0.0
    _CACHE_TTL = 300.0  # 5 minutes
    _lock: Optional[asyncio.Lock] = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        """Lazy initializer for the refresh lock."""
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    async def get_policies(cls) -> Dict[str, PolicyDefinition]:
        """Get all active policies, reloading from DB if cache expired.

        Returns:
            Dict mapping table_name to PolicyDefinition.
        """
        now = time.time()
        if not cls._policies or (now - cls._last_load_time) > cls._CACHE_TTL:
            async with cls._get_lock():
                # Double-check inside lock
                now = time.time()
                if not cls._policies or (now - cls._last_load_time) > cls._CACHE_TTL:
                    timeout_ms = get_env_int("AGENT_CONTROL_PLANE_TIMEOUT_MS", 1000)
                    try:
                        await asyncio.wait_for(cls._refresh_policies(), timeout=timeout_ms / 1000.0)
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"Policy refresh timed out after {timeout_ms}ms. "
                            "Using existing or default policies."
                        )
                        if not cls._policies:
                            cls._policies = cls._get_default_policies()
                    except Exception as e:
                        logger.error(f"Policy refresh failed: {e}")
                        if not cls._policies:
                            cls._policies = cls._get_default_policies()

        return cls._policies

    @classmethod
    async def _refresh_policies(cls) -> None:
        """Reload policies from the control-plane database."""
        query = """
            SELECT table_name, tenant_column, policy_expression
            FROM row_policies
            WHERE is_enabled = TRUE
        """

        try:
            if not ControlPlaneDatabase.is_enabled():
                # Fallback to hardcoded defaults if isolation is disabled or DB not reachable
                # This aligns with the transition plan where we might run without
                # control-plane initially or in legacy mode.
                cls._policies = cls._get_default_policies()
                cls._last_load_time = time.time()
                return

            if not ControlPlaneDatabase._pool:
                # Try to init if not already (might happen if agent runs separately)
                try:
                    await ControlPlaneDatabase.init()
                except Exception:
                    # If generic init fails (e.g. env vars missing), fall back to defaults
                    logger.warning(
                        "Could not initialize ControlPlaneDatabase, " "using default policies."
                    )
                    cls._policies = cls._get_default_policies()
                    cls._last_load_time = time.time()
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

            cls._policies = new_policies
            cls._last_load_time = time.time()
            logger.info(f"Loaded {len(cls._policies)} row policies from control-plane.")

        except Exception as e:
            logger.error(f"Failed to load row policies: {e}")
            # retain existing policies if refresh fails
            if not cls._policies:
                cls._policies = cls._get_default_policies()

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
