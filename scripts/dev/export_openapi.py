import json
import os

# Set dummy environment variables for initialization
os.environ["POSTGRES_URL"] = "postgresql://user:pass@localhost/dbname"
os.environ["CONTROL_DB_HOST"] = "localhost"
os.environ["OPENAI_API_KEY"] = "sk-dummy"

from agent_service.app import app as agent_app  # noqa: E402
from otel_worker.app import app as otel_app  # noqa: E402
from ui_api_gateway.app import app as ui_app  # noqa: E402


def export_openapi():
    """Export OpenAPI schemas from backend services to JSON files."""
    # OTEL Worker
    with open("ui/src/gen/otel_openapi.json", "w") as f:
        json.dump(otel_app.openapi(), f, indent=2)
    print("✓ Exported OTEL OpenAPI")

    # UI API Gateway
    with open("ui/src/gen/ui_openapi.json", "w") as f:
        json.dump(ui_app.openapi(), f, indent=2)
    print("✓ Exported UI OpenAPI")

    # Agent Service
    with open("ui/src/gen/agent_openapi.json", "w") as f:
        json.dump(agent_app.openapi(), f, indent=2)
    print("✓ Exported Agent OpenAPI")


if __name__ == "__main__":
    import os

    os.makedirs("ui/src/gen", exist_ok=True)
    export_openapi()
