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
    error: Optional[str] = None
    pattern_count: Optional[int] = None


class PatternReloadService:
    """Service to handle reloading of NLP patterns."""

    @staticmethod
    async def reload() -> ReloadResult:
        """
        Reload NLP patterns from the database via the CanonicalizationService.

        Returns:
            ReloadResult containing success status, timestamp, count, and error details.
        """
        logger.info("Starting pattern reload via PatternReloadService")
        try:
            service = CanonicalizationService.get_instance()
            count = await service.reload_patterns()

            logger.info(f"Pattern reload completed successfully. Count: {count}")
            return ReloadResult(
                success=True,
                error=None,
                reloaded_at=datetime.now(timezone.utc),
                pattern_count=count,
            )
        except Exception as e:
            logger.error(f"Pattern reload failed: {e}", exc_info=True)
            return ReloadResult(
                success=False,
                error=str(e),
                reloaded_at=datetime.now(timezone.utc),
                pattern_count=None,
            )
