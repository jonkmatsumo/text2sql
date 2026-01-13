import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService

logger = logging.getLogger(__name__)


@dataclass
class ReloadResult:
    """Result of a pattern reload operation."""

    success: bool
    reloaded_at: datetime
    reload_id: str
    duration_ms: float
    error: Optional[str] = None
    pattern_count: Optional[int] = None


class PatternReloadService:
    """Service to handle reloading of NLP patterns."""

    @staticmethod
    async def reload(source: str = "service") -> ReloadResult:
        """
        Reload NLP patterns from the database via the CanonicalizationService.

        Args:
            source: Trigger source identifier (e.g. "admin_tool").

        Returns:
            ReloadResult containing success status, timestamp, count, and error details.
        """
        import time
        import uuid

        reload_id = str(uuid.uuid4())
        start_time = time.perf_counter()

        logger.info(
            f"Starting pattern reload via PatternReloadService "
            f"(source={source}, reload_id={reload_id})"
        )
        try:
            service = CanonicalizationService.get_instance()
            count = await service.reload_patterns()

            duration_ms = (time.perf_counter() - start_time) * 1000

            logger.info(
                f"Pattern reload completed successfully "
                f"(source={source}, reload_id={reload_id}, duration_ms={duration_ms:.2f}). "
                f"Count: {count}"
            )
            return ReloadResult(
                success=True,
                error=None,
                reloaded_at=datetime.now(timezone.utc),
                pattern_count=count,
                reload_id=reload_id,
                duration_ms=duration_ms,
            )
        except Exception as e:
            duration_ms = (time.perf_counter() - start_time) * 1000

            logger.error(
                f"Pattern reload failed (source={source}, reload_id={reload_id}, "
                f"duration_ms={duration_ms:.2f}): {e}",
                exc_info=True,
            )
            return ReloadResult(
                success=False,
                error=str(e),
                reloaded_at=datetime.now(timezone.utc),
                pattern_count=None,
                reload_id=reload_id,
                duration_ms=duration_ms,
            )
