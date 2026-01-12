import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from otel_worker.ingestion.processor import coordinator
from otel_worker.otlp.parser import (
    extract_trace_summaries,
    parse_otlp_json_traces,
    parse_otlp_traces,
)
from otel_worker.storage.minio import init_minio
from otel_worker.storage.postgres import init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for storage and background workers."""
    try:
        init_db()
        init_minio()
    except Exception as e:
        logger.error(f"Failed to initialize storage: {e}")

    await coordinator.start()
    yield
    await coordinator.stop()


app = FastAPI(title="OTEL Dual-Write Worker", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/v1/traces")
async def receive_traces(request: Request):
    """Endpoint for OTLP traces (supports Protobuf and JSON)."""
    content_type = request.headers.get("content-type", "")

    # Normalize content-type (handle cases like 'application/json; charset=UTF-8')
    base_content_type = content_type.split(";")[0].strip().lower()

    body = await request.body()
    try:
        if base_content_type == "application/x-protobuf":
            parsed_data = parse_otlp_traces(body)
        elif base_content_type == "application/json":
            parsed_data = parse_otlp_json_traces(body)
        else:
            msg = (
                f"Unsupported content-type: '{content_type}'. "
                "Supported formats: 'application/x-protobuf', 'application/json'."
            )
            logger.warning(msg)
            return Response(
                content=msg,
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            )

        summaries = extract_trace_summaries(parsed_data)

        if not summaries:
            return Response(status_code=status.HTTP_200_OK)

        # Enqueue for background persistence
        await coordinator.enqueue(parsed_data, summaries)

        return Response(status_code=status.HTTP_202_ACCEPTED)
    except ValueError as e:
        logger.error(f"Validation failed: {e}")
        return Response(
            content=str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    except Exception as e:
        logger.error(f"Internal error processing traces: {e}")
        return Response(
            content=f"Internal Server Error: {e}",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
