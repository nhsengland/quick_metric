"""Test the new generate_metrics function."""

import pandas as pd
import pytest

from quick_metric import generate_metrics
from quick_metric.core import interpret_metric_instructions, read_metric_instructions


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
        "metric_name, method_name, expected_value",
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

        with pytest.raises(ValueError, match="Config must be a pathlib.Path"):
            generate_metrics(data, 123)  # type: ignore

    def test_generate_metrics_non_dict_instructions(self):
        """Test generate_metrics with non-dictionary metric instructions."""
        from quick_metric.core import interpret_metric_instructions

        data = pd.DataFrame({"col": [1, 2, 3]})

        with pytest.raises(ValueError, match="metric_instructions must be a dictionary"):
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
        from quick_metric.core import interpret_metric_instructions

        data = pd.DataFrame({"col": [1, 2, 3]})

        with pytest.raises(ValueError, match=expected_error):
            interpret_metric_instructions(data, config)


class TestReadMetricInstructions:
    """Test cases for read_metric_instructions function."""

    def test_read_nonexistent_file(self):
        """Test reading from nonexistent file raises FileNotFoundError."""
        from pathlib import Path
        from quick_metric.core import read_metric_instructions

        nonexistent_path = Path("/nonexistent/path/config.yaml")

        with pytest.raises(FileNotFoundError) as exc_info:
            read_metric_instructions(nonexistent_path)

        assert "Configuration file not found" in str(exc_info.value)

    def test_read_invalid_yaml(self):
        """Test reading invalid YAML raises ValueError."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")  # Invalid YAML
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError) as exc_info:
                read_metric_instructions(temp_path)

            assert "Invalid YAML in configuration file" in str(exc_info.value)
        finally:
            temp_path.unlink()  # Clean up

    def test_read_non_dict_yaml(self):
        """Test reading YAML that's not a dict raises ValueError."""
        import tempfile
        import yaml
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(["list", "instead", "of", "dict"], f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError) as exc_info:
                read_metric_instructions(temp_path)

            assert "Configuration file must contain a YAML dictionary" in str(exc_info.value)
        finally:
            temp_path.unlink()  # Clean up

    def test_read_empty_metric_instructions(self):
        """Test reading YAML with no metric_instructions logs warning."""
        import tempfile
        import yaml
        from pathlib import Path

        config_data = {"some_other_key": "value"}

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = Path(f.name)

        try:
            # This should return empty dict and log warning
            result = read_metric_instructions(temp_path)
            assert result == {}
        finally:
            temp_path.unlink()  # Clean up


class TestInterpretMetricInstructions:
    """Test cases for interpret_metric_instructions function."""

    def test_empty_instructions(self):
        """Test with empty metric instructions."""
        from quick_metric.core import interpret_metric_instructions

        data = pd.DataFrame({"col": [1, 2, 3]})

        result = interpret_metric_instructions(data, {})
        assert result == {}

    def test_with_custom_metrics_methods(self):
        """Test interpret_metric_instructions with custom methods dict."""
        from quick_metric.core import interpret_metric_instructions

        def custom_method(data):
            return len(data) * 2

        data = pd.DataFrame({"category": ["A", "A", "B"], "value": [1, 2, 3]})

        config = {"test_metric": {"method": ["custom_method"], "filter": {"category": "A"}}}

        custom_methods = {"custom_method": custom_method}

        result = interpret_metric_instructions(data, config, custom_methods)

        # Should have 2 rows with category "A", so custom_method returns 2 * 2 = 4
        assert result["test_metric"]["custom_method"] == 4
