"""
Sanity checks for data_engineering.py — FPI grouping affects merge_asof and horizons.
"""
import pytest
from data_engineering import group_fpi


def test_group_fpi_horizons_sanity():
    """FPI 6,7,8 → same group; 1,2 → same group (paper horizon logic)."""
    assert group_fpi(6) == group_fpi(7) == group_fpi(8) == "678"
    assert group_fpi(1) == group_fpi(2) == "12"
