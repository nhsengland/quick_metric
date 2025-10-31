"""
Unit tests for data splitting functionality.

Tests the split_by feature that adds dimensions to metric results.
"""

import pandas as pd
import pytest

from quick_metric.core import interpret_metric_instructions
from quick_metric.exceptions import MetricSpecificationError
from quick_metric.registry import metric_method
from quick_metric.results import DataFrameResult, ScalarResult, SeriesResult


@pytest.fixture
def test_data():
    """Create test data with regional and site dimensions."""
    return pd.DataFrame(
        {
            "region": ["R1", "R1", "R2", "R2", "R3"],
            "site": ["A", "B", "A", "B", "A"],
            "category": ["X", "Y", "X", "Y", "X"],
            "value": [10, 20, 30, 40, 50],
        }
    )


@pytest.fixture
def metric_methods():
    """Register test metric methods."""

    @metric_method
    def count_records(data):
        return len(data)

    @metric_method
    def sum_values(data):
        return data["value"].sum()

    @metric_method
    def category_counts(data):
        return data.groupby("category").size()

    return {
        "count_records": count_records,
        "sum_values": sum_values,
        "category_counts": category_counts,
    }


class TestSingleSplit:
    """Test splitting by a single column."""

    def test_scalar_to_series_with_single_split(self, test_data, metric_methods):
        """Scalar result should become SeriesResult when split by one column."""
        config = {"split_by": "region", "total_count": {"method": ["count_records"], "filter": {}}}

        store = interpret_metric_instructions(test_data, config, metric_methods)
        result = store["total_count", "count_records"]

        assert isinstance(result, SeriesResult)
        assert result.data.index.name == "region"
        assert len(result.data) == 3
        assert result.data["R1"] == 2
        assert result.data["R2"] == 2
        assert result.data["R3"] == 1

    def test_series_to_dataframe_with_single_split(self, test_data, metric_methods):
        """Series result should become DataFrameResult when split."""
        config = {
            "split_by": "region",
            "category_counts": {"method": ["category_counts"], "filter": {}},
        }

        store = interpret_metric_instructions(test_data, config, metric_methods)
        result = store["category_counts", "category_counts"]

        assert isinstance(result, DataFrameResult)
        assert "region" in result.data.index.names
        assert "category" in result.data.index.names


class TestMultipleSplits:
    """Test splitting by multiple columns."""

    def test_scalar_to_dataframe_with_multiple_splits(self, test_data, metric_methods):
        """Scalar result should become DataFrameResult with MultiIndex for multiple splits."""
        config = {
            "split_by": ["region", "site"],
            "total_count": {"method": ["count_records"], "filter": {}},
        }

        store = interpret_metric_instructions(test_data, config, metric_methods)
        result = store["total_count", "count_records"]

        assert isinstance(result, DataFrameResult)
        assert result.data.index.names == ["region", "site"]
        assert len(result.data) == 5  # 5 unique combinations
        assert result.data.loc[("R1", "A"), "value"] == 1
        assert result.data.loc[("R2", "B"), "value"] == 1

    def test_series_to_dataframe_with_multiple_splits(self, test_data, metric_methods):
        """Series result should become DataFrameResult with hierarchical index."""
        config = {
            "split_by": ["region", "site"],
            "category_counts": {"method": ["category_counts"], "filter": {}},
        }

        store = interpret_metric_instructions(test_data, config, metric_methods)
        result = store["category_counts", "category_counts"]

        assert isinstance(result, DataFrameResult)
        # Should have region, site, and category in the index
        assert len(result.data.index.names) == 3


class TestGlobalAndMetricLevelSplit:
    """Test interaction between global and metric-level split_by."""

    def test_global_split_applies_to_all_metrics(self, test_data, metric_methods):
        """Global split_by should apply to all metrics."""
        config = {
            "split_by": "region",
            "count": {"method": ["count_records"], "filter": {}},
            "sum": {"method": ["sum_values"], "filter": {}},
        }

        store = interpret_metric_instructions(test_data, config, metric_methods)

        count_result = store["count", "count_records"]
        sum_result = store["sum", "sum_values"]

        assert isinstance(count_result, SeriesResult)
        assert isinstance(sum_result, SeriesResult)
        assert count_result.data.index.name == "region"
        assert sum_result.data.index.name == "region"

    def test_metric_level_overrides_global_split(self, test_data, metric_methods):
        """Metric-level split_by should override global."""
        config = {
            "split_by": "region",
            "regional": {"method": ["count_records"], "filter": {}},
            "granular": {
                "method": ["count_records"],
                "filter": {},
                "split_by": ["region", "site"],  # Override
            },
        }

        store = interpret_metric_instructions(test_data, config, metric_methods)

        regional = store["regional", "count_records"]
        granular = store["granular", "count_records"]

        assert isinstance(regional, SeriesResult)
        assert isinstance(granular, DataFrameResult)
        assert granular.data.index.names == ["region", "site"]

    def test_metric_level_can_disable_split(self, test_data, metric_methods):
        """Metric can override global split_by with None to disable splitting."""
        config = {
            "split_by": "region",
            "split_metric": {"method": ["count_records"], "filter": {}},
            "no_split": {
                "method": ["count_records"],
                "filter": {},
                "split_by": None,  # Override to disable
            },
        }

        store = interpret_metric_instructions(test_data, config, metric_methods)

        split_result = store["split_metric", "count_records"]
        no_split_result = store["no_split", "count_records"]

        assert isinstance(split_result, SeriesResult)
        assert isinstance(no_split_result, ScalarResult)
        assert no_split_result.data == 5


class TestSplitWithFilters:
    """Test that splitting works correctly with filters."""

    def test_split_after_filter(self, test_data, metric_methods):
        """Split should be applied to filtered data."""
        config = {
            "split_by": "region",
            "x_category_count": {"method": ["count_records"], "filter": {"category": "X"}},
        }

        store = interpret_metric_instructions(test_data, config, metric_methods)
        result = store["x_category_count", "count_records"]

        assert isinstance(result, SeriesResult)
        # Only X category data should be counted
        assert result.data["R1"] == 1  # One X in R1
        assert result.data["R2"] == 1  # One X in R2
        assert result.data["R3"] == 1  # One X in R3


class TestSplitValidation:
    """Test validation of split_by configuration."""

    def test_invalid_split_column_raises_error(self, test_data, metric_methods):
        """Should raise error if split_by column doesn't exist."""
        config = {
            "split_by": "nonexistent_column",
            "metric": {"method": ["count_records"], "filter": {}},
        }

        with pytest.raises(MetricSpecificationError, match="split_by columns not found"):
            interpret_metric_instructions(test_data, config, metric_methods)

    def test_invalid_split_type_raises_error(self, test_data, metric_methods):
        """Should raise error for invalid split_by type."""
        config = {
            "split_by": 123,  # Invalid type
            "metric": {"method": ["count_records"], "filter": {}},
        }

        with pytest.raises(MetricSpecificationError, match="split_by must be str, list"):
            interpret_metric_instructions(test_data, config, metric_methods)

    def test_split_by_list_with_non_strings_raises_error(self, test_data, metric_methods):
        """Should raise error if split_by list contains non-strings."""
        config = {
            "split_by": ["region", 123],  # Invalid item
            "metric": {"method": ["count_records"], "filter": {}},
        }

        with pytest.raises(MetricSpecificationError, match="must contain only strings"):
            interpret_metric_instructions(test_data, config, metric_methods)


class TestSplitEdgeCases:
    """Test edge cases in splitting functionality."""

    def test_split_with_single_group(self, metric_methods):
        """Should handle case where split creates only one group."""
        data = pd.DataFrame({"region": ["R1", "R1", "R1"], "value": [10, 20, 30]})

        config = {"split_by": "region", "count": {"method": ["count_records"], "filter": {}}}

        store = interpret_metric_instructions(data, config, metric_methods)
        result = store["count", "count_records"]

        assert isinstance(result, SeriesResult)
        assert len(result.data) == 1
        assert result.data["R1"] == 3

    def test_split_with_empty_groups_after_filter(self, test_data, metric_methods):
        """Should handle case where some splits have no data after filtering."""
        config = {
            "split_by": "region",
            "filtered_count": {
                "method": ["count_records"],
                "filter": {"category": "Z"},  # No Z category exists
            },
        }

        store = interpret_metric_instructions(test_data, config, metric_methods)
        # Result should exist but be empty
        assert ("filtered_count", "count_records") not in store._results

    def test_split_preserves_value_column_name(self, test_data, metric_methods):
        """Should preserve value column name through splits."""
        config = {"split_by": "region", "sum": {"method": ["sum_values"], "filter": {}}}

        store = interpret_metric_instructions(test_data, config, metric_methods)
        result = store["sum", "sum_values"]

        assert isinstance(result, SeriesResult)
        # Check that sum is correct per region
        assert result.data["R1"] == 30  # 10 + 20
        assert result.data["R2"] == 70  # 30 + 40
        assert result.data["R3"] == 50
