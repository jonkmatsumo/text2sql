import gzip
import io
import json
import logging
from datetime import datetime, timezone

from minio import Minio
from otel_worker.config import settings

logger = logging.getLogger(__name__)

# Initialize MinIO client
client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=False,  # Assuming internal dev setup
)


def init_minio():
    """Ensure the OTEL bucket exists."""
    if not client.bucket_exists(settings.MINIO_BUCKET):
        client.make_bucket(settings.MINIO_BUCKET)
        logger.info(f"Created MinIO bucket: {settings.MINIO_BUCKET}")


def upload_trace_blob(trace_id: str, service_name: str, payload_dict: dict) -> str:
    """
    Upload gzipped JSON payload to MinIO.

    Returns the object path.
    """
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y-%m-%d")
    object_name = f"{settings.OTEL_ENVIRONMENT}/{service_name}/{date_path}/{trace_id}.json.gz"

    # Prepare payload
    payload_json = json.dumps(payload_dict)
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as f:
        f.write(payload_json.encode("utf-8"))

    buffer.seek(0)
    data_bytes = buffer.getvalue()

    client.put_object(
        settings.MINIO_BUCKET,
        object_name,
        io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/json",
        content_encoding="gzip",
    )

    logger.info(f"Uploaded trace blob to MinIO: {object_name}")
    return f"s3://{settings.MINIO_BUCKET}/{object_name}"
