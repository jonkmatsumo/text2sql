import json
import shutil
import tempfile
from pathlib import Path

import pytest

from text2sql_synth.config import SynthConfig
from text2sql_synth.export import export_to_directory
from text2sql_synth.orchestrator import generate_tables


@pytest.fixture
def temp_out_dir():
    """Create a temporary directory for export tests."""
    dir_path = tempfile.mkdtemp()
    yield Path(dir_path)
    shutil.rmtree(dir_path)


def test_export_structure(temp_out_dir):
    """Verify that export creates the correct subdirectory structure."""
    cfg = SynthConfig.preset("small")
    cfg.seed = 42
    cfg.output.csv = True
    cfg.output.parquet = True

    # Generate a subset to keep it fast
    ctx, _ = generate_tables(cfg, only=["dim_time", "dim_institution"])

    manifest_path = export_to_directory(ctx, cfg, temp_out_dir)

    assert manifest_path.exists()
    assert (temp_out_dir / "csv").is_dir()
    assert (temp_out_dir / "parquet").is_dir()
    assert (temp_out_dir / "csv" / "dim_time.csv").exists()
    assert (temp_out_dir / "parquet" / "dim_time.parquet").exists()
    assert (temp_out_dir / "csv" / "dim_institution.csv").exists()
    assert (temp_out_dir / "parquet" / "dim_institution.parquet").exists()


def test_export_determinism(temp_out_dir):
    """Verify that exporting twice with same seed yields identical content hash."""
    cfg = SynthConfig.preset("small")
    cfg.seed = 123

    # First run
    out1 = temp_out_dir / "run1"
    ctx1, _ = generate_tables(cfg, only=["dim_institution"])
    m1_path = export_to_directory(ctx1, cfg, out1)

    with open(m1_path) as f:
        m1 = json.load(f)

    # Second run
    out2 = temp_out_dir / "run2"
    ctx2, _ = generate_tables(cfg, only=["dim_institution"])
    m2_path = export_to_directory(ctx2, cfg, out2)

    with open(m2_path) as f:
        m2 = json.load(f)

    # Compare content hashes (should be identical)
    assert m1["content_hash"] == m2["content_hash"]
    assert m1["seed"] == m2["seed"]
    assert m1["schema_snapshot_id"] == m2["schema_snapshot_id"]

    # Compare file hashes specifically
    f1 = {f["file"]: f["hash"] for f in m1["files"]}
    f2 = {f["file"]: f["hash"] for f in m2["files"]}
    assert f1 == f2


def test_export_determinism_different_seed(temp_out_dir):
    """Verify that different seeds yield different content hashes."""
    cfg = SynthConfig.preset("small")

    # Run 1
    cfg.seed = 1
    out1 = temp_out_dir / "run1"
    ctx1, _ = generate_tables(cfg, only=["dim_institution"])
    m1_path = export_to_directory(ctx1, cfg, out1)
    with open(m1_path) as f:
        m1 = json.load(f)

    # Run 2
    cfg.seed = 2
    out2 = temp_out_dir / "run2"
    ctx2, _ = generate_tables(cfg, only=["dim_institution"])
    m2_path = export_to_directory(ctx2, cfg, out2)
    with open(m2_path) as f:
        m2 = json.load(f)

    assert m1["content_hash"] != m2["content_hash"]
