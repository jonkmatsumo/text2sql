import logging

from fastapi import FastAPI, Request, Response, status
from otel_worker.otlp.parser import extract_trace_summaries, parse_otlp_traces

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="OTEL Dual-Write Worker")


@app.get("/healthz")
async def healthz():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/v1/traces")
async def receive_traces(request: Request):
    """Endpoint for OTLP/HTTP protobuf traces."""
    content_type = request.headers.get("content-type")

    # We primarily support binary protobuf as specified
    if content_type == "application/x-protobuf":
        body = await request.body()
        try:
            parsed_data = parse_otlp_traces(body)
            summaries = extract_trace_summaries(parsed_data)

            logger.info(f"Received {len(summaries)} spans from OTLP request")

            # Phase 2: Just logging for now
            # Phase 3: Storage in Postgres/MinIO
            # Phase 4: MLflow export

            return Response(status_code=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f"Failed to parse OTLP traces: {e}")
            return Response(status_code=status.HTTP_400_BAD_REQUEST)
    else:
        logger.warning(f"Unsupported content-type: {content_type}")
        return Response(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)
