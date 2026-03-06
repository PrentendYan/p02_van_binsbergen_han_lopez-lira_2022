"""Shared fixtures for the test suite."""
import sys
from pathlib import Path

import pytest

# Make src importable
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))


@pytest.fixture(scope="session")
def project_root():
    return Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def images_dir(project_root):
    return project_root / "_output" / "images"


@pytest.fixture(scope="session")
def pdp_png(images_dir):
    return images_dir / "partial_dependence_meanest.png"
