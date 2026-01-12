import logging
import os
import time

from agent_core.telemetry import SpanType, telemetry

# Set up logging to see backend warnings if any
logging.basicConfig(level=logging.INFO)


def verify_otel():
    """Emit a test span to OTEL for verification."""
    print("--- OTEL Smoke Verification ---")

    # Force OTEL backend
    os.environ["TELEMETRY_BACKEND"] = "otel"

    # Note: Ensure OTEL_EXPORTER_OTLP_ENDPOINT is set in your shell if using a collector
    # e.g., export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"

    print(f"Backend configured: {os.environ.get('TELEMETRY_BACKEND')}")
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "Default (usually localhost:4317)")
    print(f"OTLP Endpoint: {endpoint}")

    with telemetry.start_span(
        name="smoke_test_span", span_type=SpanType.TOOL, inputs={"purpose": "manual_verification"}
    ) as span:
        print("Span 'smoke_test_span' started. Sleeping 1s...")
        time.sleep(1)
        span.set_outputs({"status": "delivered", "timestamp": time.time()})
        span.add_event("verification_complete")
        print("Span completed.")

    print("\nVerification finished.")
    print("If a collector is running, check for 'smoke_test_span' in your UI (Jaeger/Zipkin/etc).")


if __name__ == "__main__":
    verify_otel()
