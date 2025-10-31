"""End-to-end tests for complete workflow scenarios."""

import pytest

from quick_metric.core import interpret_metric_instructions
from quick_metric.registry import (
    _registry,
    list_method_names,
    metric_method,
)
from quick_metric.store import MetricsStore


class TestCompleteWorkflow:
    """Test complete workflows from YAML config to final results."""

    def test_yaml_to_results_returns_expected_structure(self, basic_results):
        """Test that YAML workflow returns correct result structure."""
        expected_keys = {"cancer_analysis", "high_value_cases"}
        assert isinstance(basic_results, MetricsStore)
        assert set(basic_results.metrics()) == expected_keys

    @pytest.mark.parametrize(
        ("metric_name", "expected_methods"),
        [
            ("cancer_analysis", {"count_records", "mean_value"}),
            ("high_value_cases", {"count_records", "sum_values"}),
        ],
    )
    def test_yaml_to_results_metric_structure(self, basic_results, metric_name, expected_methods):
        """Test that each metric returns expected method results."""
        assert set(basic_results.methods(metric_name)) == expected_methods

    def test_yaml_to_results_cancer_analysis_count(self, basic_results):
        """Test that cancer analysis returns correct count."""
        # 4 cancer cases (none have remove='Remove')
        assert basic_results.value("cancer_analysis", "count_records") == 4

    def test_yaml_to_results_high_value_filtering(self, basic_results):
        """Test that high value filtering works correctly."""
        # Values > 200: [250, 300, 350, 400] = 4 records
        assert basic_results.value("high_value_cases", "count_records") == 4

    def test_custom_methods_end_to_end(self, healthcare_data):
        """Test end-to-end workflow with custom methods."""
        # Store initial state
        initial_methods = set(list_method_names())

        @metric_method
        def efficiency_score(data):
            """Calculate processing efficiency score."""
            if "processing_days" in data.columns and len(data) > 0:
                fast_processes = len(data[data["processing_days"] <= 10])
                total = len(data)
                return round(fast_processes / total * 100, 2)
            return 0.0

        try:
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
            actual_keys = set(results.methods("efficiency_analysis"))
            assert actual_keys == expected_methods
        finally:
            # Clean up custom method
            current_methods = set(list_method_names())
            new_methods = current_methods - initial_methods
            for method_name in new_methods:
                if method_name in _registry._methods:
                    del _registry._methods[method_name]

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

        assert isinstance(results, MetricsStore)
        assert ("complex_filter_metric", "count_records") in results
        count_value = results.value("complex_filter_metric", "count_records")
        assert isinstance(count_value, int)
        assert count_value >= 0

    def test_error_propagation_workflow(self, healthcare_data):
        """Test that errors are properly propagated through the workflow."""
        config = {
            "bad_metric": {
                "method": ["nonexistent_method"],
                "filter": {"cancer_rare_disease": "Cancer"},
            }
        }

        with pytest.raises(Exception, match="nonexistent_method"):
            interpret_metric_instructions(healthcare_data, config)

    def test_empty_data_workflow(self, empty_data):
        """Test workflow behavior with empty dataset."""
        config = {
            "empty_test": {
                "method": ["count_records"],
                "filter": {"col1": "any_value"},
            }
        }

        results = interpret_metric_instructions(empty_data, config)

        assert results.value("empty_test", "count_records") == 0

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
        assert set(results.metrics()) == expected_keys

    @pytest.mark.parametrize(
        ("metric_name", "expected_methods"),
        [
            ("metric_1", {"count_records"}),
            ("metric_2", {"sum_values", "mean_value"}),
            ("metric_3", {"count_records"}),
        ],
    )
    def test_multiple_metrics_individual_structure(
        self, healthcare_data, metric_name, expected_methods
    ):
        """Test each metric has correct structure in multiple metrics."""
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
        assert set(results.methods(metric_name)) == expected_methods
