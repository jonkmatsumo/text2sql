"""Canonical multi-turn conversation scripts for synthetic data evaluation."""

from pathlib import Path

import yaml


def get_catalog():
    """Load and return the multi-turn script catalog."""
    catalog_path = Path(__file__).parent / "catalog.yaml"
    with open(catalog_path, "r") as f:
        return yaml.safe_load(f)
