"""Unit tests for apply_methods module."""

import pandas as pd
import pytest

from quick_metric._apply_methods import apply_method, apply_methods
from quick_metric._exceptions import (
    MetricsMethodNotFoundError,
    MetricSpecificationError,
)


class TestApplyMethod:
    """Test apply_method function."""

    @pytest.fixture
    def sample_data(self):
        """Sample DataFrame for testing."""
        return pd.DataFrame({"value": [1, 2, 3, 4, 5]})

    def test_apply_method_calls_correct_function(self, sample_data):
        """Test that apply_method calls the correct method function."""

        def test_method(data):
            return len(data)

        methods_dict = {"test_method": test_method}
        result_key, result_value = apply_method(sample_data, "test_method", methods_dict)

        assert result_key == "test_method"
        assert result_value == 5

    def test_apply_method_raises_error_for_missing_method(self, sample_data):
        """Test that apply_method raises error for non-existent method."""
        methods_dict = {"existing_method": lambda x: len(x)}

        with pytest.raises(MetricsMethodNotFoundError):
            apply_method(sample_data, "missing_method", methods_dict)

    def test_apply_method_uses_default_methods_when_none_provided(self, sample_data):
        """Test that apply_method uses METRICS_METHODS when none provided."""
        # Just test that it doesn't crash and uses the real methods
        result_key, result_value = apply_method(sample_data, "count_records")
        assert result_key == "count_records"
        assert result_value == 5

    def test_apply_method_passes_through_method_return_value(self, sample_data):
        """Test that apply_method returns exactly what the method returns."""

        def complex_method(data):
            return {"mean": data["value"].mean(), "count": len(data)}

        methods_dict = {"test_method": complex_method}
        result_key, result_value = apply_method(sample_data, "test_method", methods_dict)

        assert result_key == "test_method"
        assert result_value == {"mean": 3.0, "count": 5}

    def test_apply_method_propagates_method_exceptions(self, sample_data):
        """Test that exceptions from methods are propagated correctly."""

        def failing_method(_data):
            raise ValueError("Method error")

        methods_dict = {"test_method": failing_method}

        with pytest.raises(ValueError, match="Method error"):
            apply_method(sample_data, "test_method", methods_dict)

    def test_apply_method_with_dict_method_spec_single_param(self, sample_data):
        """Test apply_method with dict method spec containing single parameter."""

        def param_method(data, column="value"):
            return data[column].sum()

        methods_dict = {"param_method": param_method}
        method_spec = {"param_method": {"column": "value"}}

        result_key, result_value = apply_method(sample_data, method_spec, methods_dict)

        assert result_key == "param_method_columnvalue"
        assert result_value == 15  # sum of [1,2,3,4,5]

    def test_apply_method_with_dict_method_spec_no_params(self, sample_data):
        """Test apply_method with dict method spec containing empty parameters."""

        def simple_method(data):
            return len(data)

        methods_dict = {"simple_method": simple_method}
        method_spec = {"simple_method": {}}

        result_key, result_value = apply_method(sample_data, method_spec, methods_dict)

        assert result_key == "simple_method"
        assert result_value == 5

    def test_apply_method_with_dict_method_spec_complex_params(self, sample_data):
        """Test apply_method with dict spec for hash generation with many parameters."""

        def complex_method(_data, **kwargs):
            return len(kwargs)

        methods_dict = {"complex_method": complex_method}
        # Create a method spec with many parameters to trigger hash generation
        complex_params = {f"param_{i}": f"value_{i}" for i in range(20)}
        method_spec = {"complex_method": complex_params}

        result_key, result_value = apply_method(sample_data, method_spec, methods_dict)

        assert result_key.startswith("complex_method_")
        assert len(result_key.split("_")) == 3  # complex_method_{hash}
        assert result_value == 20

    def test_apply_method_with_invalid_dict_method_spec_multiple_methods(self, sample_data):
        """Test apply_method raises error for dict with multiple methods."""
        methods_dict = {"method1": lambda _: 1, "method2": lambda _: 2}
        method_spec = {"method1": {}, "method2": {}}

        with pytest.raises(MetricSpecificationError, match="exactly one method"):
            apply_method(sample_data, method_spec, methods_dict)

    def test_apply_method_with_invalid_dict_method_spec_non_dict_params(self, sample_data):
        """Test apply_method raises error for dict with non-dict parameters."""
        methods_dict = {"method1": lambda _: 1}
        method_spec = {"method1": "not_a_dict"}

        with pytest.raises(MetricSpecificationError, match="must be a dictionary"):
            apply_method(sample_data, method_spec, methods_dict)

    def test_apply_method_with_invalid_method_spec_type(self, sample_data):
        """Test apply_method raises error for invalid method spec type."""
        methods_dict = {"method1": lambda _: 1}
        method_spec = 123  # Invalid type

        with pytest.raises(MetricSpecificationError, match="must be str or dict"):
            apply_method(sample_data, method_spec, methods_dict)


class TestApplyMethods:
    """Test apply_methods function."""

    @pytest.fixture
    def sample_data(self):
        """Sample DataFrame for testing."""
        return pd.DataFrame({"value": [1, 2, 3]})

    def test_apply_methods_calls_apply_method_for_each_method(self, sample_data):
        """Test that apply_methods calls apply_method for each method name."""

        def method1(data):
            return len(data)

        def method2(data):
            return data["value"].sum()

        def method3(data):
            return data["value"].mean()

        method_names = ["method1", "method2", "method3"]
        methods_dict = {
            "method1": method1,
            "method2": method2,
            "method3": method3,
        }

        result = apply_methods(sample_data, method_names, methods_dict)

        expected_result = {
            "method1": 3,
            "method2": 6,
            "method3": 2.0,
        }
        assert result == expected_result

    def test_apply_methods_passes_correct_arguments(self, sample_data):
        """Test that apply_methods passes correct arguments to apply_method."""

        def test_method(data):
            # Verify we get the right data
            assert len(data) == 3
            assert "value" in data.columns
            return "success"

        method_names = ["test_method"]
        methods_dict = {"test_method": test_method}

        result = apply_methods(sample_data, method_names, methods_dict)

        assert result == {"test_method": "success"}

    def test_apply_methods_returns_empty_dict_for_empty_method_list(self, sample_data):
        """Test that apply_methods returns empty dict for empty method list."""
        result = apply_methods(sample_data, [], {})

        assert result == {}

    def test_apply_methods_propagates_apply_method_exceptions(self, sample_data):
        """Test that exceptions from apply_method are propagated."""

        def failing_method(_data):
            raise ValueError("Test error")

        methods_dict = {"failing_method": failing_method}

        with pytest.raises(ValueError, match="Test error"):
            apply_methods(sample_data, ["failing_method"], methods_dict)

    def test_apply_methods_uses_default_methods_when_none_provided(self, sample_data):
        """Test that apply_methods uses METRICS_METHODS when none provided."""
        # Just test it doesn't crash and uses real methods
        result = apply_methods(sample_data, ["count_records"])

        assert result == {"count_records": 3}
