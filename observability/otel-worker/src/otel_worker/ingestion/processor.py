"""Background processing for OTEL trace ingestion."""

import asyncio
import base64
import logging
from typing import List, Tuple

from otel_worker.export.mlflow_exporter import export_to_mlflow
from otel_worker.storage.minio import upload_trace_blob
from otel_worker.storage.postgres import save_trace_and_spans

logger = logging.getLogger(__name__)

# Max items in memory before we start blocking or rejecting (backpressure)
MAX_QUEUE_SIZE = 1000


class PersistenceCoordinator:
    """Manages background persistence of OTEL traces with retries and isolation."""

    def __init__(self, max_attempts: int = 5, initial_delay: float = 1.0):
        """Initialize the persistence coordinator."""
        self.queue: asyncio.Queue[Tuple[dict, List[dict]]] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.worker_tasks: List[asyncio.Task] = []
        self._stopping = False

    async def start(self, num_workers: int = 2):
        """Start background worker tasks."""
        self._stopping = False
        for i in range(num_workers):
            task = asyncio.create_task(self._worker(i))
            self.worker_tasks.append(task)
        logger.info(f"Started {num_workers} background persistence workers")

    async def stop(self):
        """Stop background worker tasks gracefully."""
        self._stopping = True
        logger.info("Stopping background workers, awaiting pending tasks...")
        # Give periodic yields to allow worker to process the last item if it just started
        if self.queue.qsize() > 0:
            await self.queue.join()

        for task in self.worker_tasks:
            task.cancel()
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        logger.info("Background workers stopped")

    async def enqueue(self, parsed_data: dict, summaries: List[dict]):
        """Enqueue a batch of traces for background processing."""
        try:
            # We enqueue the whole request batch to preserve atomicity if needed,
            # but the worker will split them by trace_id for processing.
            self.queue.put_nowait((parsed_data, summaries))
        except asyncio.QueueFull:
            logger.error("Persistence queue is full, dropping trace batch!")
            raise ValueError("Queue overflow")

    async def _worker(self, worker_id: int):
        """Run the worker loop that processes tasks from the queue."""
        while not self._stopping:
            try:
                # Use a timeout to occasionally check _stopping
                try:
                    batch = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                parsed_data, summaries = batch
                await self._process_batch(parsed_data, summaries)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} encountered unexpected error: {e}")

    async def _process_batch(self, parsed_data: dict, summaries: List[dict]):
        """Split batch by trace_id and process each trace independently."""
        trace_ids = set(s["trace_id"] for s in summaries)
        for tid_b64 in trace_ids:
            try:
                tid_bytes = base64.b64decode(tid_b64)
                trace_id = tid_bytes.hex()
                trace_summaries = [s for s in summaries if s["trace_id"] == tid_b64]
                service_name = trace_summaries[0]["service_name"]

                # Launch independent tasks for each trace to isolate failures
                await self._process_trace(trace_id, service_name, parsed_data, trace_summaries)
            except Exception as e:
                logger.error(f"Failed to initiate trace processing for {tid_b64}: {e}")

    async def _process_trace(
        self, trace_id: str, service_name: str, parsed_data: dict, summaries: List[dict]
    ):
        """Process a single trace through all sinks with isolation and retries."""

        async def store_persistent():
            # 1. MinIO + Postgres (Sequential because Postgres needs raw_blob_url)
            # We wrap them in a single retry block because they are tightly coupled via the URL
            # Run blocking IO in threads
            raw_blob_url = await asyncio.to_thread(
                upload_trace_blob, trace_id, service_name, parsed_data
            )
            await asyncio.to_thread(
                save_trace_and_spans, trace_id, parsed_data, summaries, raw_blob_url
            )

        # 2. MLflow (Independent)
        async def export_mlflow():
            await asyncio.to_thread(
                export_to_mlflow, trace_id, service_name, summaries, parsed_data
            )

        # Run sinks in parallel
        results = await asyncio.gather(
            self._run_with_retry(store_persistent, f"Storage (MinIO/PG) - {trace_id}"),
            self._run_with_retry(export_mlflow, f"Export (MLflow) - {trace_id}"),
            return_exceptions=True,
        )

        for i, res in enumerate(results):
            if isinstance(res, Exception):
                sink_name = "Storage" if i == 0 else "MLflow"
                logger.error(f"Sink {sink_name} failed permanently for trace {trace_id}: {res}")

    async def _run_with_retry(self, func, label: str):
        """Run a function with exponential backoff retries."""
        delay = self.initial_delay
        for attempt in range(1, self.max_attempts + 1):
            try:
                return await func()
            except Exception as e:
                if attempt == self.max_attempts:
                    logger.error(f"[{label}] Final attempt {attempt} failed: {e}")
                    raise
                logger.warning(f"[{label}] Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff


coordinator = PersistenceCoordinator()
