import asyncio
import logging
from datetime import datetime, timedelta, timezone

from dal.database import Database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def cleanup_stale_runs(hours: int = 24):
    """Mark AWAITING_REVIEW ingestion runs older than N hours as FAILED."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    logger.info(f"Cleaning up ingestion runs created before {cutoff}")

    try:
        await Database.init()
        async with Database.get_connection(tenant_id=1) as conn:
            # We mark as FAILED with an error message indicating expiry
            res = await conn.execute(
                """
                UPDATE nlp_pattern_runs
                SET status = 'FAILED',
                    error_message = $1,
                    completed_at = NOW()
                WHERE status = 'AWAITING_REVIEW'
                  AND started_at < $2
                """,
                f"Expired: Automatically closed after {hours}h of inactivity.",
                cutoff,
            )

            count = 0
            if res.startswith("UPDATE "):
                count = int(res.split(" ")[1])

            logger.info(f"Successfully cleaned up {count} stale ingestion runs.")

    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
    finally:
        await Database.close()


if __name__ == "__main__":
    asyncio.run(cleanup_stale_runs())
