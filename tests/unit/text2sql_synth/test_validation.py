import tempfile
from pathlib import Path

import pandas as pd

from text2sql_synth.cli import build_parser
from text2sql_synth.validate import _check_fk_integrity, validate_manifest


def test_validation_on_small_preset():
    """Test that validation passes on a fresh 'small' preset generation."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = Path(tmp_dir) / "output"

        # Run generation
        parser = build_parser()
        args = parser.parse_args(["generate", "--preset", "small", "--out", str(out_path)])
        args.func(args)

        manifest_path = out_path / "manifest.json"
        assert manifest_path.exists()

        # Run validation
        result = validate_manifest(manifest_path)

        assert result.is_valid, f"Validation failed with errors: {result.errors}"
        assert "distributions" in result.metrics
        assert "aggregates" in result.metrics
        assert "correlations" in result.metrics

        # Check if report was generated
        report_path = out_path / "validation_report.md"
        assert report_path.exists()


def test_fk_integrity_catches_orphans():
    """Test that _check_fk_integrity catches orphaned records."""
    # Create sample tables
    customers = pd.DataFrame(
        {"customer_id": ["cust_1", "cust_2"], "primary_address_id": ["addr_1", "addr_2"]}
    )

    addresses = pd.DataFrame({"address_id": ["addr_1"]})  # addr_2 is missing

    tables = {"dim_customer": customers, "dim_address": addresses}

    errors = _check_fk_integrity(tables)

    assert any("dim_customer.primary_address_id" in err for err in errors)
    assert any("orphaned values" in err for err in errors)


def test_fk_integrity_passes_on_valid_data():
    """Test that _check_fk_integrity passes when all FKs are valid."""
    customers = pd.DataFrame(
        {"customer_id": ["cust_1", "cust_2"], "primary_address_id": ["addr_1", "addr_2"]}
    )

    addresses = pd.DataFrame({"address_id": ["addr_1", "addr_2"]})

    tables = {"dim_customer": customers, "dim_address": addresses}

    errors = _check_fk_integrity(tables)
    assert len(errors) == 0


def test_cli_validate_command():
    """Test the CLI 'validate' command directly."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        out_path = Path(tmp_dir) / "output"
        out_path.mkdir()

        # Create a dummy manifest and some data
        manifest_path = out_path / "manifest.json"
        with open(manifest_path, "w") as f:
            f.write(
                '{"files": [{"table": "dim_time", "file": "csv/dim_time.csv", "format": "csv"}]}'
            )

        csv_dir = out_path / "csv"
        csv_dir.mkdir()
        pd.DataFrame({"date_key": [1], "full_date": ["2024-01-01"]}).to_csv(
            csv_dir / "dim_time.csv", index=False
        )

        parser = build_parser()
        args = parser.parse_args(["validate", "--manifest", str(manifest_path)])

        # This should return 0 (success) as there are no FKs to check for dim_time
        exit_code = args.func(args)
        assert exit_code == 0
