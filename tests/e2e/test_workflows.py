"""End-to-end tests for complete workflow scenarios."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from quick_metric import (
    interpret_metric_instructions,
    metric_method,
    read_metric_instructions,
)


class TestCompleteWorkflow:
    """Test complete workflows from YAML config to final results."""

    @pytest.fixture
    def healthcare_data(self):
        """Realistic healthcare dataset for testing."""
        return pd.DataFrame(
            {
                "raw_local_point_of_delivery_code": (
                    ["TESTREPORT"] * 6 + ["TESTPLATE"] * 2
                ),
                "test_directory_test_method": ["WGS"] * 4 + ["Panel"] * 4,
                "cancer_rare_disease": ["Cancer"] * 4 + ["Rare Disease"] * 4,
                "specialist_test_group_test_code": (
                    ["Core"] * 3 + ["Haematological Tumours"] * 2 + ["Other"] * 3
                ),
                "remove": ["Keep"] * 7 + ["Remove"],
                "processing_days": [5, 15, 8, 22, 3, 18, 12, 25],
                "value": [100, 200, 150, 300, 120, 250, 180, 400],
            }
        )

    @pytest.fixture
    def yaml_config_file(self):
        """Create temporary YAML config file."""
        config_data = {
            "metric_instructions": {
                "cancer_analysis": {
                    "method": ["count_records", "mean_value"],
                    "filter": {
                        "and": {
                            "cancer_rare_disease": "Cancer",
                            "not": {"remove": "Remove"},
                        }
                    },
                },
                "high_value_cases": {
                    "method": ["count_records", "sum_values"],
                    "filter": {"value": {"greater than": 200}},
                },
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            yield Path(f.name)

        # Cleanup
        Path(f.name).unlink(missing_ok=True)

    def test_yaml_to_results_returns_expected_structure(
        self, healthcare_data, yaml_config_file
    ):
        """Test that YAML workflow returns correct result structure."""
        instructions = read_metric_instructions(yaml_config_file)
        results = interpret_metric_instructions(healthcare_data, instructions)

        expected_keys = {"cancer_analysis", "high_value_cases"}
        assert isinstance(results, dict)
        assert set(results.keys()) == expected_keys

    @pytest.mark.parametrize(
        "metric_name,expected_methods",
        [
            ("cancer_analysis", {"count_records", "mean_value"}),
            ("high_value_cases", {"count_records", "sum_values"}),
        ],
    )
    def test_yaml_to_results_metric_structure(
        self, healthcare_data, yaml_config_file, metric_name, expected_methods
    ):
        """Test that each metric returns expected method results."""
        instructions = read_metric_instructions(yaml_config_file)
        results = interpret_metric_instructions(healthcare_data, instructions)

        assert isinstance(results[metric_name], dict)
        assert set(results[metric_name].keys()) == expected_methods

    def test_yaml_to_results_cancer_analysis_count(
        self, healthcare_data, yaml_config_file
    ):
        """Test that cancer analysis returns correct count."""
        instructions = read_metric_instructions(yaml_config_file)
        results = interpret_metric_instructions(healthcare_data, instructions)

        # 4 cancer cases (none have remove='Remove')
        assert results["cancer_analysis"]["count_records"] == 4

    def test_yaml_to_results_high_value_filtering(
        self, healthcare_data, yaml_config_file
    ):
        """Test that high value filtering works correctly."""
        instructions = read_metric_instructions(yaml_config_file)
        results = interpret_metric_instructions(healthcare_data, instructions)

        # Values > 200: [300, 250, 400] = 3 records
        assert results["high_value_cases"]["count_records"] == 3

    def test_custom_methods_end_to_end(self, healthcare_data):
        """Test end-to-end workflow with custom methods."""

        @metric_method
        def efficiency_score(data):
            """Calculate processing efficiency score."""
            if "processing_days" in data.columns and len(data) > 0:
                fast_processes = len(data[data["processing_days"] <= 10])
                total = len(data)
                return round(fast_processes / total * 100, 2)
            return 0.0

        config = {
            "efficiency_analysis": {
                "method": ["efficiency_score", "count_records"],
                "filter": {
                    "and": {
                        "raw_local_point_of_delivery_code": "TESTREPORT",
                        "not": {"remove": "Remove"},
                    }
                },
            }
        }

        results = interpret_metric_instructions(healthcare_data, config)

        expected_methods = {"efficiency_score", "count_records"}
        assert set(results["efficiency_analysis"].keys()) == expected_methods

    def test_complex_filtering_workflow(self, healthcare_data):
        """Test workflow with complex nested filters."""
        config = {
            "complex_filter_metric": {
                "method": ["count_records"],
                "filter": {
                    "or": [
                        {
                            "and": {
                                "cancer_rare_disease": "Cancer",
                                "processing_days": {"less than": 10},
                            }
                        },
                        {
                            "and": {
                                "test_directory_test_method": "WGS",
                                "value": {"greater than": 250},
                            }
                        },
                    ]
                },
            }
        }

        results = interpret_metric_instructions(healthcare_data, config)

        assert "complex_filter_metric" in results
        metric_result = results["complex_filter_metric"]
        assert "count_records" in metric_result
        assert isinstance(metric_result["count_records"], int)
        assert metric_result["count_records"] >= 0

    def test_error_propagation_workflow(self, healthcare_data):
        """Test that errors are properly propagated through the workflow."""
        config = {
            "bad_metric": {
                "method": ["nonexistent_method"],
                "filter": {"cancer_rare_disease": "Cancer"},
            }
        }

        with pytest.raises(Exception) as exc_info:
            interpret_metric_instructions(healthcare_data, config)

        # Should contain information about the missing method
        assert "nonexistent_method" in str(exc_info.value)

    def test_empty_data_workflow(self):
        """Test workflow behavior with empty dataset."""
        empty_data = pd.DataFrame(columns=["col1", "col2"])

        config = {
            "empty_test": {"method": ["count_records"], "filter": {"col1": "any_value"}}
        }

        results = interpret_metric_instructions(empty_data, config)

        assert results["empty_test"]["count_records"] == 0

    def test_multiple_metrics_workflow(self, healthcare_data):
        """Test workflow with multiple different metric types."""
        config = {
            "metric_1": {
                "method": ["count_records"],
                "filter": {"cancer_rare_disease": "Cancer"},
            },
            "metric_2": {
                "method": ["sum_values", "mean_value"],
                "filter": {"test_directory_test_method": "WGS"},
            },
            "metric_3": {
                "method": ["count_records"],
                "filter": {
                    "and": {
                        "processing_days": {"less than": 20},
                        "not": {"remove": "Remove"},
                    }
                },
            },
        }

        results = interpret_metric_instructions(healthcare_data, config)

        expected_keys = {"metric_1", "metric_2", "metric_3"}
        assert set(results.keys()) == expected_keys

    @pytest.mark.parametrize(
        "metric_name,expected_methods",
        [
            ("metric_1", {"count_records"}),
            ("metric_2", {"sum_values", "mean_value"}),
            ("metric_3", {"count_records"}),
        ],
    )
    def test_multiple_metrics_individual_structure(
        self, healthcare_data, metric_name, expected_methods
    ):
        """Test each metric has correct structure in multiple metrics workflow."""
        config = {
            "metric_1": {
                "method": ["count_records"],
                "filter": {"cancer_rare_disease": "Cancer"},
            },
            "metric_2": {
                "method": ["sum_values", "mean_value"],
                "filter": {"test_directory_test_method": "WGS"},
            },
            "metric_3": {
                "method": ["count_records"],
                "filter": {
                    "and": {
                        "processing_days": {"less than": 20},
                        "not": {"remove": "Remove"},
                    }
                },
            },
        }

        results = interpret_metric_instructions(healthcare_data, config)
        assert set(results[metric_name].keys()) == expected_methods
