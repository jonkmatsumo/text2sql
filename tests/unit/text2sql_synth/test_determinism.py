import json
from pathlib import Path

import pytest

from text2sql_synth.config import SynthConfig
from text2sql_synth.orchestrator import generate_tables
from text2sql_synth.util.hashing import stable_hash_str

GOLDEN_PATH = Path("golden/mvp_digest.txt")


def compute_mvp_digest():
    """Compute a stable digest for the MVP dataset."""
    config = SynthConfig.preset("mvp")
    # Disable parquet for the regression test as it may be non-deterministic
    config.output.parquet = False

    ctx, tables = generate_tables(config)

    digest_parts = []

    # 1. Schema Snapshot ID
    digest_parts.append(f"schema:{ctx.schema_snapshot_id}")

    # 2. Config Snapshot (normalized)
    config_dict = config.to_dict()
    # Ensure nested dicts are also sorted if using json.dumps(sort_keys=True)
    config_json = json.dumps(config_dict, sort_keys=True)
    digest_parts.append(f"config:{stable_hash_str(config_json)}")

    # 3. Per-table content hash
    # Table processing order must be stable
    for table_name in sorted(tables.keys()):
        df = tables[table_name]

        # Ensure column order is stable
        df = df.reindex(sorted(df.columns), axis=1)

        # Convert to a stable string representation
        # We use CSV format with fixed line endings and no index
        content = df.to_csv(index=False, lineterminator="\n")
        table_hash = stable_hash_str(content)

        digest_parts.append(f"table:{table_name}:{table_hash}")

    # Combine all parts into a final digest
    final_digest = stable_hash_str("|".join(digest_parts))
    return final_digest


def test_mvp_determinism_regression():
    """Assert that the MVP dataset digest matches the stored golden value."""
    # This path is relative to the synthetic-data directory where pytest is run
    golden_file = Path(__file__).parent.parent / "golden/mvp_digest.txt"

    actual_digest = compute_mvp_digest()

    if not golden_file.exists():
        # If the file doesn't exist, we allow creating it once
        # In a real CI environment, this should fail if the file is missing
        golden_file.parent.mkdir(parents=True, exist_ok=True)
        with open(golden_file, "w") as f:
            f.write(actual_digest)
        pytest.skip(f"Golden file not found. Created initial digest: {actual_digest}")

    with open(golden_file, "r") as f:
        expected_digest = f.read().strip()

    assert actual_digest == expected_digest, (
        f"MVP dataset digest mismatch!\n"
        f"Expected: {expected_digest}\n"
        f"Actual:   {actual_digest}\n"
        f"If this change was intentional, update {golden_file}"
    )


if __name__ == "__main__":
    # Convenience for generating the initial digest
    print(compute_mvp_digest())
