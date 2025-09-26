"""Test data utilities for quick_metric tests."""

import pandas as pd
import pytest


def create_sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "raw_local_point_of_delivery_code": [
                "TESTREPORT",
                "TESTREPORT",
                "OTHER",
                "TESTREPORT",
            ],
            "test_directory_test_method": ["WGS", "NOT_WGS", "WGS", "NOT_WGS"],
            "cancer_rare_disease": ["Rare Disease", "Cancer", "Rare Disease", "Cancer"],
            "specialist_test_group_test_code": [
                "Core",
                "Core/Specialised",
                "Other",
                "Core",
            ],
            "remove": ["Keep", "Keep", "Remove", "Keep"],
            "value": [10, 20, 30, 40],
        }
    )


@pytest.fixture
def sample_dataframe():
    """Pytest fixture for sample DataFrame."""
    return create_sample_dataframe()
