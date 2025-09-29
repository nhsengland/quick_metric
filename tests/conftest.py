"""Pytest configuration and fixtures for quick_metric tests."""

import pandas as pd
import pytest

from quick_metric.method_definitions import metric_method


# Test helper methods - only registered when running tests
@metric_method
def count_records(data: pd.DataFrame) -> int:
    """Count the number of records in the data."""
    return len(data)


@metric_method
def mean_value(data: pd.DataFrame, column: str = "value") -> float:
    """Calculate the mean of a specified column."""
    if column in data.columns:
        return data[column].mean()
    return 0.0


@metric_method
def sum_values(data: pd.DataFrame, column: str = "value") -> float:
    """Calculate the sum of a specified column."""
    if column in data.columns:
        return data[column].sum()
    return 0.0


@metric_method
def describe_data(data: pd.DataFrame) -> pd.DataFrame:
    """Return descriptive statistics for the data."""
    return data.describe()


@pytest.fixture
def sample_dataframe():
    """Pytest fixture for sample DataFrame."""
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
