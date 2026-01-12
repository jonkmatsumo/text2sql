import base64
import logging

from fastapi import FastAPI, Request, Response, status
from otel_worker.export.mlflow_exporter import export_to_mlflow
from otel_worker.otlp.parser import extract_trace_summaries, parse_otlp_traces
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
    """Endpoint for OTLP/HTTP protobuf traces."""
    content_type = request.headers.get("content-type")

    if content_type == "application/x-protobuf":
        body = await request.body()
        try:
            parsed_data = parse_otlp_traces(body)
            summaries = extract_trace_summaries(parsed_data)

            if not summaries:
                return Response(status_code=status.HTTP_200_OK)

            # Process each trace separately for storage
            # (In a real high-load worker, this would be queued or batched differently)
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
        except Exception as e:
            logger.error(f"Failed to process OTLP traces: {e}")
            return Response(status_code=status.HTTP_400_BAD_REQUEST)
    else:
        logger.warning(f"Unsupported content-type: {content_type}")
        return Response(status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE)
