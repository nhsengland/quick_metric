"""Test core functionality."""

from pathlib import Path
import tempfile

import pandas as pd
import pytest
import yaml

from quick_metric import generate_metrics
from quick_metric._core import (
    _normalize_method_specs,
    interpret_metric_instructions,
    read_metric_instructions,
)
from quick_metric._exceptions import MetricSpecificationError


class TestGenerateMetrics:
    """Test cases for the generate_metrics function."""

    def test_generate_metrics_with_dict_config(self):
        """Test generate_metrics with dictionary configuration."""
        results = self._run_generate_metrics_test()

        # Basic smoke test - ensure it returns a dict with expected structure
        assert isinstance(results, dict)
        assert len(results) == 2

    @pytest.mark.parametrize("expected_metric", ["category_a_metrics", "category_b_metrics"])
    def test_generate_metrics_contains_expected_metrics(self, expected_metric):
        """Test that generate_metrics returns expected metric keys."""
        results = self._run_generate_metrics_test()
        assert expected_metric in results

    @pytest.mark.parametrize(
        ("metric_name", "method_name", "expected_value"),
        [
            ("category_a_metrics", "count_records", 3),
            ("category_a_metrics", "sum_values", 30),
            ("category_b_metrics", "count_records", 2),
            ("category_b_metrics", "sum_values", 45),
        ],
    )
    def test_generate_metrics_method_values(self, metric_name, method_name, expected_value):
        """Test that generate_metrics returns correct method values."""
        results = self._run_generate_metrics_test()
        assert results[metric_name][method_name] == expected_value

    def _run_generate_metrics_test(self):
        """Helper method to run the generate_metrics test scenario."""
        # Create test data
        data = pd.DataFrame(
            {
                "category": ["A", "B", "A", "C", "B", "A"],
                "value": [10, 20, 15, 30, 25, 5],
            }
        )

        # Test with dictionary configuration
        config = {
            "category_a_metrics": {
                "method": ["count_records", "sum_values"],
                "filter": {"category": "A"},
            },
            "category_b_metrics": {
                "method": ["count_records", "sum_values"],
                "filter": {"category": "B"},
            },
        }

        return generate_metrics(data, config)

    def test_generate_metrics_with_empty_filter(self):
        """Test generate_metrics with empty filter (all data)."""
        data = pd.DataFrame({"category": ["A", "B"], "value": [10, 20]})

        config = {"all_records": {"method": ["count_records"], "filter": {}}}

        results = generate_metrics(data, config)
        assert results["all_records"]["count_records"] == 2

    def test_generate_metrics_invalid_config_type(self):
        """Test generate_metrics with invalid config type."""
        data = pd.DataFrame({"col": [1, 2, 3]})

        with pytest.raises(MetricSpecificationError, match="Config must be a pathlib.Path"):
            generate_metrics(data, 123)  # type: ignore

    def test_generate_metrics_non_dict_instructions(self):
        """Test generate_metrics with non-dictionary metric instructions."""

        data = pd.DataFrame({"col": [1, 2, 3]})

        with pytest.raises(
            MetricSpecificationError, match="metric_instructions must be a dictionary"
        ):
            interpret_metric_instructions(data, "not_a_dict")  # type: ignore

    def test_generate_metrics_empty_dataframe_warning(self):
        """Test generate_metrics logs warning with empty DataFrame."""
        empty_data = pd.DataFrame()
        config = {"test": {"method": ["count_records"], "filter": {}}}

        # Should handle empty dataframe gracefully
        result = generate_metrics(empty_data, config)
        assert result == {"test": {"count_records": 0}}

    @pytest.mark.parametrize(
        ("config", "expected_error"),
        [
            ({"metric1": "not_a_dict"}, "Metric 'metric1' instruction must be a dictionary"),
            (
                {"metric2": {"filter": {}}},  # Missing 'method'
                "Metric 'metric2' missing required 'method' key",
            ),
            (
                {"metric3": {"method": ["count_records"]}},  # Missing 'filter'
                "Metric 'metric3' missing required 'filter' key",
            ),
        ],
    )
    def test_generate_metrics_invalid_structure(self, config, expected_error):
        """Test generate_metrics with invalid metric instruction structure."""

        data = pd.DataFrame({"col": [1, 2, 3]})

        with pytest.raises(MetricSpecificationError, match=expected_error):
            interpret_metric_instructions(data, config)

    def test_generate_metrics_invalid_output_format(self):
        """Test generate_metrics with invalid output format string."""
        data = pd.DataFrame({"col": [1, 2, 3]})
        config = {"test": {"method": ["count_records"], "filter": {}}}

        with pytest.raises(
            MetricSpecificationError, match="Invalid output_format 'invalid_format'"
        ):
            generate_metrics(data, config, output_format="invalid_format")

    def test_generate_metrics_invalid_config_type_detailed(self):
        """Test generate_metrics with invalid config type (not Path or dict)."""
        data = pd.DataFrame({"col": [1, 2, 3]})
        invalid_config = ["not", "a", "dict", "or", "path"]

        with pytest.raises(
            MetricSpecificationError, match="Config must be a pathlib.Path object or dict"
        ):
            generate_metrics(data, invalid_config)  # type: ignore

    def test_generate_metrics_with_flat_output_format(self):
        """Test generate_metrics with flat_dataframe output format conversion."""
        data = pd.DataFrame({"category": ["A", "B"], "value": [10, 20]})
        config = {"test_metric": {"method": ["count_records"], "filter": {}}}

        result = generate_metrics(data, config, output_format="flat_dataframe")

        # Should be a DataFrame (flat format)
        assert isinstance(result, pd.DataFrame)

    def test_generate_metrics_with_records_output_format(self):
        """Test generate_metrics with records output format conversion."""
        data = pd.DataFrame({"category": ["A", "B"], "value": [10, 20]})
        config = {"test_metric": {"method": ["count_records"], "filter": {}}}

        result = generate_metrics(data, config, output_format="records")

        # Should be a list of dictionaries
        assert isinstance(result, list)
        assert len(result) > 0
        assert isinstance(result[0], dict)


class TestReadMetricInstructions:
    """Test cases for read_metric_instructions function."""

    def test_read_nonexistent_file(self):
        """Test reading from nonexistent file raises FileNotFoundError."""

        nonexistent_path = Path("/nonexistent/path/config.yaml")

        with pytest.raises(FileNotFoundError) as exc_info:
            read_metric_instructions(nonexistent_path)

        assert "Configuration file not found" in str(exc_info.value)

    def test_invalid_yaml_raises_syntax_error(self):
        """Test that invalid YAML raises a MetricSpecificationError."""
        # Create a temporary file with invalid YAML content
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_file:
            temp_file.write("invalid: yaml: content: [")
            temp_file_path = Path(temp_file.name)

        try:
            with pytest.raises(
                MetricSpecificationError, match="Invalid YAML in configuration file"
            ):
                read_metric_instructions(temp_file_path)
        finally:
            # Clean up
            temp_file_path.unlink()

    def test_read_non_dict_yaml(self):
        """Test reading from file that contains non-dict YAML raises MetricSpecificationError."""
        # Create a temporary file with list content
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_file:
            yaml.dump([1, 2, 3], temp_file)
            temp_file_path = Path(temp_file.name)

        try:
            with pytest.raises(
                MetricSpecificationError, match="Configuration file must contain a YAML dictionary"
            ):
                read_metric_instructions(temp_file_path)
        finally:
            # Clean up
            temp_file_path.unlink()

    def test_read_empty_metric_instructions(self):
        """Test reading empty metric instructions returns empty dict."""
        # Create a temporary file with empty dict
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_file:
            yaml.dump({}, temp_file)
            temp_file_path = Path(temp_file.name)

        try:
            result = read_metric_instructions(temp_file_path)
            assert result == {}
        finally:
            # Clean up
            temp_file_path.unlink()


class TestInterpretMetricInstructions:
    """Test cases for interpret_metric_instructions function."""

    def test_empty_instructions(self):
        """Test with empty metric instructions."""

        data = pd.DataFrame({"col": [1, 2, 3]})

        result = interpret_metric_instructions(data, {})
        assert result == {}

    def test_with_custom_metrics_methods(self):
        """Test interpret_metric_instructions with custom methods dict."""

        def custom_method(data):
            return len(data) * 2

        data = pd.DataFrame({"category": ["A", "A", "B"], "value": [1, 2, 3]})

        config = {"test_metric": {"method": ["custom_method"], "filter": {"category": "A"}}}

        custom_methods = {"custom_method": custom_method}

        result = interpret_metric_instructions(data, config, custom_methods)

        # Should have 2 rows with category "A", so custom_method returns 2 * 2 = 4
        assert result["test_metric"]["custom_method"] == 4


class TestNormalizeMethodSpecs:
    """Test cases for the _normalize_method_specs function."""

    def test_normalize_method_specs_with_invalid_list_item(self):
        """Test _normalize_method_specs with invalid item in list."""
        # List with invalid item type
        invalid_method_input = ["valid_method", 123]  # 123 is invalid

        with pytest.raises(MetricSpecificationError, match="Method list items must be str or dict"):
            _normalize_method_specs(invalid_method_input)

    def test_normalize_method_specs_with_invalid_type(self):
        """Test _normalize_method_specs with completely invalid type."""
        # Invalid type (not str, list, or dict)
        invalid_method_input = 123

        with pytest.raises(
            MetricSpecificationError, match="Method specification must be str, list, or dict"
        ):
            _normalize_method_specs(invalid_method_input)

    def test_normalize_method_specs_with_valid_string(self):
        """Test _normalize_method_specs with valid string."""
        result = _normalize_method_specs("test_method")
        assert result == ["test_method"]

    def test_normalize_method_specs_with_valid_list(self):
        """Test _normalize_method_specs with valid list."""
        method_input = ["method1", {"method2": {"param": "value"}}]
        result = _normalize_method_specs(method_input)
        assert result == ["method1", {"method2": {"param": "value"}}]

    def test_normalize_method_specs_with_valid_dict(self):
        """Test _normalize_method_specs with valid dict."""
        method_input = {"test_method": {"param": "value"}}
        result = _normalize_method_specs(method_input)
        assert result == [{"test_method": {"param": "value"}}]
