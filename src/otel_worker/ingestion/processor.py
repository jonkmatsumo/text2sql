"""Background processing for OTEL trace ingestion."""

import asyncio
import base64
import logging
import time
from typing import Dict, List, Set

from otel_worker.config import settings
from otel_worker.logging import log_event
from otel_worker.otlp.parser import (
    extract_trace_summaries,
    parse_otlp_json_traces,
    parse_otlp_traces,
)
from otel_worker.storage.minio import upload_trace_blob
from otel_worker.storage.postgres import (
    poll_ingestion_queue,
    save_traces_batch,
    update_ingestion_status,
)

logger = logging.getLogger(__name__)


class PersistenceCoordinator:
    """Manages background persistence of OTEL traces with durable buffering and batching."""

    def __init__(self, max_attempts: int = 5, initial_delay: float = 1.0):
        """Initialize the persistence coordinator."""
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.worker_tasks: List[asyncio.Task] = []
        self._stopping = False
        # Bound concurrent MinIO uploads to avoid task explosion
        self._minio_semaphore = asyncio.Semaphore(10)

    async def start(self, num_workers: int = 2):
        """Start background worker tasks."""
        if any(not task.done() for task in self.worker_tasks):
            logger.debug("Persistence workers already running")
            return
        self._stopping = False
        self.worker_tasks = []
        for i in range(num_workers):
            task = asyncio.create_task(self._worker(i))
            self.worker_tasks.append(task)
        logger.info(
            f"Started {num_workers} background persistence workers "
            f"(Batch: {settings.BATCH_MAX_SIZE}, Interval: {settings.BATCH_FLUSH_INTERVAL_MS}ms)"
        )

    async def stop(self):
        """Stop background worker tasks gracefully."""
        self._stopping = True
        if not self.worker_tasks:
            return
        logger.info("Stopping background workers...")
        for task in self.worker_tasks:
            task.cancel()
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.worker_tasks = []
        logger.info("Background workers stopped")

    async def enqueue(self, *args, **kwargs):
        """Legacy method for in-memory enqueueing. Now a no-op as app.py writes to DB."""
        pass

    async def _worker(self, worker_id: int):
        """Run the worker loop that polls the ingestion queue and groups into batches."""
        buffer: List[dict] = []
        last_flush = time.time()

        while not self._stopping:
            try:
                # 0. Backpressure Check (Memory)
                # If buffer is full, pause polling to allow processing to catch up
                if len(buffer) >= settings.PROCESSING_QUEUE_MAX_DEPTH:
                    log_event(
                        "processing_paused",
                        reason="buffer_full",
                        buffer_size=len(buffer),
                    )
                    await asyncio.sleep(0.1)
                    # Continue to flush check
                else:
                    # Poll for pending items up to remaining batch capacity
                    # We still use BATCH_MAX_SIZE for the *chunk* we pull,
                    # but limit total buffer by PROCESSING_QUEUE_MAX_DEPTH
                    capacity = settings.PROCESSING_QUEUE_MAX_DEPTH - len(buffer)
                    poll_limit = min(capacity, settings.BATCH_MAX_SIZE)

                    if poll_limit > 0:
                        items = await asyncio.to_thread(poll_ingestion_queue, limit=poll_limit)
                        if items:
                            buffer.extend(items)

                now = time.time()
                interval_ms = (now - last_flush) * 1000

                # Check flush conditions: size or interval
                if buffer and (
                    len(buffer) >= settings.BATCH_MAX_SIZE
                    or interval_ms >= settings.BATCH_FLUSH_INTERVAL_MS
                ):
                    try:
                        await self._process_batch(buffer)
                    finally:
                        # Clear buffer regardless of success
                        # (individual items handle retries via DB status)
                        buffer = []
                        last_flush = now
                elif not buffer:
                    # Nothing to do, wait for new items
                    await asyncio.sleep(1.0)
                else:
                    # Buffer exists but not ready to flush, wait a bit
                    await asyncio.sleep(0.05)

            except asyncio.CancelledError:
                if buffer:
                    await self._process_batch(buffer)
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} encountered unexpected error: {e}")
                await asyncio.sleep(5.0)

    async def _process_batch(self, items: List[dict]):
        """Process a list of items from the ingestion queue as a single batch."""
        # 1. Parse and group by trace_id
        # trace_id -> {'parsed_data': dict, 'summaries': list, 'service_name': str, 'item_ids': set}
        trace_map: Dict[str, dict] = {}
        processed_item_ids: Set[int] = set()

        # Collect all item IDs for failure handling
        all_item_ids = {item["id"] for item in items}

        try:
            for item in items:
                item_id = item["id"]
                processed_item_ids.add(item_id)
                payload = item["payload_json"]

                try:
                    # Reconstruct original body and parse
                    body = base64.b64decode(payload["body_b64"])
                    content_type = payload["content_type"]

                    if content_type == "application/x-protobuf":
                        parsed_data = parse_otlp_traces(body)
                    else:
                        parsed_data = parse_otlp_json_traces(body)

                    summaries = extract_trace_summaries(parsed_data)
                    if not summaries:
                        # Empty or no-op payload, mark as complete later
                        continue

                    # Group by trace_id found in summaries
                    # (Normally one OTLP request has multiple traces)
                    trace_summary_ids = set(s["trace_id"] for s in summaries)
                    for tid_b64 in trace_summary_ids:
                        tid_bytes = base64.b64decode(tid_b64)
                        trace_id = tid_bytes.hex()
                        trace_summaries = [s for s in summaries if s["trace_id"] == tid_b64]
                        service_name = trace_summaries[0]["service_name"]

                        if trace_id not in trace_map:
                            trace_map[trace_id] = {
                                "parsed_data": parsed_data,  # Ref to full data
                                "summaries": [],
                                "service_name": service_name,
                                "item_ids": set(),
                            }
                        trace_map[trace_id]["summaries"].extend(trace_summaries)
                        trace_map[trace_id]["item_ids"].add(item_id)

                except Exception as e:
                    logger.error(f"Failed to parse ingestion item {item_id}: {e}")
                    await asyncio.to_thread(
                        update_ingestion_status, item_id, "failed", error=str(e)
                    )
                    # Remove from processed so we don't double-mark
                    processed_item_ids.remove(item_id)
                    all_item_ids.remove(item_id)

            if not trace_map:
                # If nothing to persist, just mark the remaining items complete
                for item_id in set(all_item_ids).intersection(processed_item_ids):
                    await asyncio.to_thread(update_ingestion_status, item_id, "complete")
                return

            # 2. Sequential/Parallel Processing Stages
            # A) MinIO Uploads (Concurrent but bounded)
            # B) Postgres Save (Batched transaction)

            # Stage A: MinIO Uploads (Parallel, Bounded)
            async def bounded_upload(tid, svc, data):
                async with self._minio_semaphore:
                    return await asyncio.to_thread(upload_trace_blob, tid, svc, data)

            upload_tasks = []
            trace_id_list = list(trace_map.keys())
            for tid in trace_id_list:
                t_ctx = trace_map[tid]
                upload_tasks.append(
                    bounded_upload(tid, t_ctx["service_name"], t_ctx["parsed_data"])
                )

            # We use return_exceptions=True to capture failures per trace
            blob_urls = await asyncio.gather(*upload_tasks, return_exceptions=True)

            # Stage B: Postgres Batch Save
            # Collect trace units for those that uploaded successfully
            trace_units = []
            successful_trace_ids = []
            for i, url in enumerate(blob_urls):
                tid = trace_id_list[i]
                if isinstance(url, Exception):
                    logger.error(f"MinIO upload failed for trace {tid}: {url}")
                    continue

                trace_units.append(
                    {
                        "trace_id": tid,
                        "summaries": trace_map[tid]["summaries"],
                        "raw_blob_url": url,
                    }
                )
                successful_trace_ids.append(tid)

            # Perform the batched write
            if trace_units:
                await self._run_with_retry(
                    lambda: asyncio.to_thread(save_traces_batch, trace_units), "Postgres Batch Save"
                )

            # 3. Finalize items
            # Mark all items as complete
            for item_id in set(all_item_ids).intersection(processed_item_ids):
                await asyncio.to_thread(update_ingestion_status, item_id, "complete")
                log_event("item_complete", item_id=item_id)

        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            log_event("batch_persist_failed", error=str(e), batch_size=len(items))
            # Mark all involved items as failed so they can be retried by DB with backoff
            for item_id in all_item_ids:
                try:
                    await asyncio.to_thread(
                        update_ingestion_status, item_id, "failed", error=str(e)
                    )
                except Exception:
                    pass

    async def _run_with_retry(self, func, label: str):
        """Run a function with exponential backoff retries."""
        delay = self.initial_delay
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = func()
                if asyncio.iscoroutine(result):
                    return await result
                return result
            except Exception as e:
                if attempt == self.max_attempts:
                    logger.error(f"[{label}] Final attempt {attempt} failed: {e}")
                    raise
                logger.warning(f"[{label}] Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff


coordinator = PersistenceCoordinator()
