"""Pytest configuration and fixtures for quick_metric tests."""

import pandas as pd
import pytest
import yaml

from quick_metric.core import (
    interpret_metric_instructions,
    read_metric_instructions,
)
from quick_metric.registry import METRICS_METHODS, metric_method


@pytest.fixture(autouse=True)
def preserve_test_methods():
    """Preserve and restore test methods around each test.

    This ensures that test methods registered in conftest are available
    even if individual tests call clear_methods().
    """
    # Save current state before test
    saved_methods = METRICS_METHODS.copy()

    yield

    # Restore saved methods after test
    METRICS_METHODS.clear()
    METRICS_METHODS.update(saved_methods)


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
        # Use Python's sum() to avoid numpy/pandas compatibility issues
        return float(sum(data[column]))
    return 0.0


@metric_method
def describe_data(data: pd.DataFrame) -> pd.DataFrame:
    """Return descriptive statistics for the data."""
    return data.describe()


@pytest.fixture
def sample_dataframe():
    """Pytest fixture for simple sample DataFrame."""
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
def healthcare_data():
    """Realistic healthcare dataset for testing workflows."""
    return pd.DataFrame(
        {
            "raw_local_point_of_delivery_code": (["TESTREPORT"] * 6 + ["TESTPLATE"] * 2),
            "test_directory_test_method": ["WGS"] * 4 + ["Panel"] * 4,
            "cancer_rare_disease": ["Cancer"] * 4 + ["Rare Disease"] * 4,
            "specialist_test_group_test_code": (
                ["Core"] * 3 + ["Haematological Tumours"] * 2 + ["Other"] * 3
            ),
            "remove": ["Keep"] * 7 + ["Remove"],
            "processing_days": [5, 15, 8, 22, 3, 18, 12, 25],
            "value": [100, 150, 200, 250, 300, 350, 400, 50],
        }
    )


@pytest.fixture
def basic_yaml_config(tmp_path):
    """Create a basic YAML config file for testing."""
    config_data = {
        "metric_instructions": {
            "cancer_analysis": {
                "method": ["count_records", "mean_value"],
                "filter": {"cancer_rare_disease": "Cancer"},
            },
            "high_value_cases": {
                "method": ["count_records", "sum_values"],
                "filter": {"value": {"greater than": 200}},
            },
        }
    }

    config_file = tmp_path / "config.yaml"
    with open(config_file, "w", encoding="utf-8") as f:
        yaml.dump(config_data, f)

    return config_file


@pytest.fixture
def yaml_instructions(basic_yaml_config):
    """Load instructions from YAML config."""
    return read_metric_instructions(basic_yaml_config)


@pytest.fixture
def basic_results(healthcare_data, yaml_instructions):
    """Execute basic workflow and return results."""
    return interpret_metric_instructions(healthcare_data, yaml_instructions)


@pytest.fixture
def create_yaml_config(tmp_path):
    """Factory fixture to create custom YAML config files."""

    def _create_config(config_data):
        config_file = tmp_path / f"config_{id(config_data)}.yaml"
        with open(config_file, "w", encoding="utf-8") as f:
            yaml.dump(config_data, f)
        return config_file

    return _create_config


@pytest.fixture
def empty_data():
    """Empty DataFrame for testing edge cases."""
    return pd.DataFrame(columns=["col1", "col2"])
