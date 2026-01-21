import sys
from pathlib import Path

INGESTION_SRC = Path(__file__).resolve().parents[2] / "ingestion" / "src"
sys.path.insert(0, str(INGESTION_SRC))
sys.modules.pop("ingestion", None)
