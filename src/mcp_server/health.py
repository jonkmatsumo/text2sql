"""Health and initialization state tracking for MCP server.

This module provides structured tracking of startup initialization steps
to enable meaningful health endpoint responses.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class CheckStatus(Enum):
    """Status of an initialization check."""

    PENDING = "pending"
    OK = "ok"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class CheckResult:
    """Result of a single initialization check."""

    name: str
    status: CheckStatus = CheckStatus.PENDING
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    timestamp: Optional[datetime] = None
    required: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "name": self.name,
            "status": self.status.value,
            "required": self.required,
        }
        if self.error_type:
            result["error_type"] = self.error_type
        if self.error_message:
            result["error_message"] = self.error_message
        if self.timestamp:
            result["timestamp"] = self.timestamp.isoformat()
        return result


@dataclass
class InitializationState:
    """Track the state of MCP server initialization.

    Records the success or failure of each startup step to enable
    meaningful health/readiness endpoint responses.
    """

    checks: Dict[str, CheckResult] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def start(self) -> None:
        """Mark initialization as started."""
        self.started_at = datetime.now(timezone.utc)

    def complete(self) -> None:
        """Mark initialization as completed."""
        self.completed_at = datetime.now(timezone.utc)

    def record_success(self, name: str, required: bool = True) -> None:
        """Record a successful initialization step.

        Args:
            name: Identifier for the initialization step.
            required: Whether this step is required for readiness.
        """
        self.checks[name] = CheckResult(
            name=name,
            status=CheckStatus.OK,
            timestamp=datetime.now(timezone.utc),
            required=required,
        )
        logger.info("Initialization step '%s' completed successfully", name)

    def record_failure(self, name: str, exc: BaseException, required: bool = True) -> None:
        """Record a failed initialization step.

        Args:
            name: Identifier for the initialization step.
            exc: The exception that caused the failure.
            required: Whether this step is required for readiness.
        """
        self.checks[name] = CheckResult(
            name=name,
            status=CheckStatus.FAILED,
            error_type=type(exc).__name__,
            error_message=str(exc),
            timestamp=datetime.now(timezone.utc),
            required=required,
        )
        logger.error(
            "Initialization step '%s' failed: %s: %s",
            name,
            type(exc).__name__,
            str(exc),
        )

    def record_skipped(self, name: str, reason: str = "") -> None:
        """Record a skipped initialization step.

        Args:
            name: Identifier for the initialization step.
            reason: Optional reason for skipping.
        """
        self.checks[name] = CheckResult(
            name=name,
            status=CheckStatus.SKIPPED,
            error_message=reason if reason else None,
            timestamp=datetime.now(timezone.utc),
            required=False,
        )
        logger.info("Initialization step '%s' skipped: %s", name, reason or "N/A")

    @property
    def is_ready(self) -> bool:
        """Indicate if server initialization completed successfully.

        Returns:
            True if all required initialization steps succeeded.
        """
        if not self.checks:
            return False

        for check in self.checks.values():
            if check.required and check.status != CheckStatus.OK:
                return False
        return True

    @property
    def failed_checks(self) -> list[CheckResult]:
        """Get list of failed initialization checks."""
        return [c for c in self.checks.values() if c.status == CheckStatus.FAILED]

    def as_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON health response.

        Returns:
            Dictionary with ready status, checks, and timing info.
        """
        return {
            "ready": self.is_ready,
            "checks": {name: check.to_dict() for name, check in self.checks.items()},
            "failed_checks": [c.to_dict() for c in self.failed_checks],
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


# Global initialization state instance for the MCP server
init_state = InitializationState()
