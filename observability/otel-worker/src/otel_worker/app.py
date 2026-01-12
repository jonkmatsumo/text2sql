import base64
import logging

from fastapi import FastAPI, Request, Response, status
from otel_worker.export.mlflow_exporter import export_to_mlflow
from otel_worker.otlp.parser import (
    extract_trace_summaries,
    parse_otlp_json_traces,
    parse_otlp_traces,
)
from otel_worker.storage.minio import init_minio, upload_trace_blob
from otel_worker.storage.postgres import init_db, save_trace_and_spans

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="OTEL Dual-Write Worker")


@app.on_event("startup")
def startup_event():
    """Initialize storage on startup."""
    try:
        init_db()
        init_minio()
    except Exception as e:
        logger.error(f"Failed to initialize storage: {e}")


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

        # Process each trace separately for storage
        trace_ids = set(s["trace_id"] for s in summaries)
        for tid_b64 in trace_ids:
            # tid_b64 is base64 encoded by MessageToDict
            tid_bytes = base64.b64decode(tid_b64)
            trace_id = tid_bytes.hex()

            trace_summaries = [s for s in summaries if s["trace_id"] == tid_b64]
            service_name = trace_summaries[0]["service_name"]

            # 1. Upload to MinIO
            raw_blob_url = upload_trace_blob(trace_id, service_name, parsed_data)

            # 2. Save to Postgres
            save_trace_and_spans(trace_id, parsed_data, trace_summaries, raw_blob_url)

            # 3. Dual-write to MLflow
            export_to_mlflow(trace_id, service_name, trace_summaries, parsed_data)

        return Response(status_code=status.HTTP_200_OK)
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
