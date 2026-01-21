"""Tests for dataset configuration warnings."""

import os
from unittest.mock import patch

import pytest

from common.config.dataset import get_dataset_mode


def test_get_dataset_mode_pagila_warning():
    """Test that Pagila mode emits a DeprecationWarning."""
    with patch.dict(os.environ, {"DATASET_MODE": "pagila"}):
        with pytest.warns(DeprecationWarning, match="Pagila dataset is deprecated"):
            mode = get_dataset_mode()
            assert mode == "pagila"
