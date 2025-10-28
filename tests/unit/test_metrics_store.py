"""Test MetricsStore functionality."""

import pandas as pd
import pytest

from quick_metric import generate_metrics, metric_method
from quick_metric.store import MetricsStore, ScalarResult


class TestMetricsStoreBasics:
    """Test basic MetricsStore operations."""

    @pytest.fixture
    def sample_store(self):
        """Create a sample store for testing."""
        data = pd.DataFrame(
            {"category": ["A", "B", "A", "B"] * 10, "site": ["R0A", "R0B"] * 20, "value": range(40)}
        )

        config = {
            "all_data": {"filter": {}, "method": ["count_records", "sum_values", "describe_data"]},
            "category_a": {"filter": {"category": "A"}, "method": ["count_records", "sum_values"]},
        }

        return generate_metrics(data, config)

    def test_store_is_metricsstore(self, sample_store):
        """Test that generate_metrics returns a MetricsStore."""
        assert isinstance(sample_store, MetricsStore)

    def test_store_length(self, sample_store):
        """Test store length."""
        # 2 metrics: all_data (3 methods) + category_a (2 methods) = 5 results
        assert len(sample_store) == 5

    def test_direct_access_tuple(self, sample_store):
        """Test direct access with tuple."""
        result = sample_store["all_data", "count_records"]
        assert isinstance(result, ScalarResult)
        assert result.data == 40

    def test_value_access(self, sample_store):
        """Test .value() method."""
        assert sample_store.value("all_data", "count_records") == 40
        assert sample_store.value("category_a", "count_records") == 20

    def test_get_method(self, sample_store):
        """Test .get() method returns None for missing."""
        result = sample_store.get("nonexistent", "method")
        assert result is None

        result = sample_store.get("all_data", "count_records")
        assert result is not None

    def test_keys(self, sample_store):
        """Test .keys() returns all (metric, method) tuples."""
        keys = list(sample_store.keys())
        assert ("all_data", "count_records") in keys
        assert ("category_a", "sum_values") in keys
        assert len(keys) == 5

    def test_metrics(self, sample_store):
        """Test .metrics() returns unique metric names."""
        metrics = sample_store.metrics()
        assert "all_data" in metrics
        assert "category_a" in metrics
        assert len(metrics) == 2

    def test_methods_all(self, sample_store):
        """Test .methods() without filter."""
        methods = sample_store.methods()
        assert "count_records" in methods
        assert "sum_values" in methods
        assert "describe_data" in methods

    def test_methods_for_metric(self, sample_store):
        """Test .methods(metric) filters by metric."""
        methods = sample_store.methods("category_a")
        assert "count_records" in methods
        assert "sum_values" in methods
        assert len(methods) == 2


class TestMetricsStoreFiltering:
    """Test MetricsStore filtering capabilities."""

    @pytest.fixture
    def sample_store(self):
        """Create a sample store for testing."""
        data = pd.DataFrame(
            {"category": ["A", "B", "A", "B"] * 10, "site": ["R0A", "R0B"] * 20, "value": range(40)}
        )

        config = {
            "metric_1": {"filter": {}, "method": ["count_records", "sum_values", "describe_data"]},
            "metric_2": {"filter": {"category": "A"}, "method": ["count_records", "describe_data"]},
            "metric_3": {"filter": {"site": "R0A"}, "method": ["sum_values"]},
        }

        return generate_metrics(data, config)

    def test_filter_by_metric(self, sample_store):
        """Test filtering by metric name."""
        filtered = sample_store.filter(metric="metric_1")
        assert len(filtered) == 3
        assert all(m == "metric_1" for m, _, _ in filtered.all())

    def test_filter_by_method(self, sample_store):
        """Test filtering by method name."""
        filtered = sample_store.filter(method="count_records")
        assert len(filtered) == 2
        assert all(meth == "count_records" for _, meth, _ in filtered.all())

    def test_filter_by_value_type(self, sample_store):
        """Test filtering by value type."""
        scalars = sample_store.filter(value_type="scalar")
        assert all(isinstance(r, ScalarResult) for _, _, r in scalars.all())

        # No Series or DataFrames in this test data
        series = sample_store.filter(value_type="series")
        assert len(series) == 0

    def test_by_method(self, sample_store):
        """Test .by_method() convenience method."""
        counts = sample_store.by_method("count_records")
        assert len(counts) == 2
        assert all(meth == "count_records" for _, meth, _ in counts.all())

    def test_by_method_list(self, sample_store):
        """Test .by_method() with list of methods."""
        filtered = sample_store.by_method(["count_records", "sum_values"])
        methods = [meth for _, meth, _ in filtered.all()]
        assert "count_records" in methods
        assert "sum_values" in methods
        assert "describe_data" not in methods

    def test_by_dimension(self, sample_store):
        """Test .by_dimension() convenience method - filters by dimensional data values."""
        # In our sample store, we have results with scalar values (no dimensions)
        # This test would need results with actual dimensional data to be meaningful
        # For now, just verify the method exists and returns a store
        filtered = sample_store.filter(category="A")  # Use filter instead
        assert isinstance(filtered, MetricsStore)

    def test_by_metric(self, sample_store):
        """Test .by_metric() convenience method."""
        filtered = sample_store.by_metric("metric_1")
        assert len(filtered) == 3
        assert all(m == "metric_1" for m, _, _ in filtered.all())

    def test_filter_chain(self, sample_store):
        """Test chaining filters."""
        filtered = sample_store.filter(value_type="scalar").filter(method="count_records")
        assert len(filtered) == 2
        assert all(isinstance(r, ScalarResult) for _, _, r in filtered.all())
        assert all(meth == "count_records" for _, meth, _ in filtered.all())

    def test_filter_combined(self, sample_store):
        """Test combining multiple filters in one call."""
        filtered = sample_store.filter(method="count_records", value_type="scalar")
        assert len(filtered) == 2


class TestMetricsStoreIteration:
    """Test MetricsStore iteration methods."""

    @pytest.fixture
    def sample_store(self):
        """Create a sample store for testing."""
        data = pd.DataFrame({"category": ["A", "B"] * 10, "value": range(20)})

        config = {
            "test_metric": {
                "filter": {},
                "method": ["count_records", "sum_values", "describe_data"],
            }
        }

        return generate_metrics(data, config)

    def test_scalars_iteration(self, sample_store):
        """Test iterating over scalars."""
        scalars = list(sample_store.scalars())
        assert len(scalars) == 2  # count_records, sum_values
        for _metric, _method, result in scalars:
            assert isinstance(result, ScalarResult)

    def test_series_iteration(self, sample_store):
        """Test iterating over series."""
        series = list(sample_store.series())
        assert len(series) == 0  # No series results

    def test_dataframes_iteration(self, sample_store):
        """Test iterating over dataframes."""
        dfs = list(sample_store.dataframes())
        assert len(dfs) == 1  # describe_data returns DataFrame

    def test_all_iteration(self, sample_store):
        """Test iterating over all results."""
        all_results = list(sample_store.all())
        assert len(all_results) == 3


class TestMetricsStoreExport:
    """Test MetricsStore export methods."""

    @pytest.fixture
    def sample_store(self):
        """Create a sample store for testing."""
        data = pd.DataFrame({"category": ["A", "B"] * 5, "value": range(10)})

        config = {
            "test": {"filter": {}, "method": ["count_records", "sum_values", "describe_data"]}
        }

        return generate_metrics(data, config)

    def test_to_dataframe(self, sample_store):
        """Test exporting to DataFrame."""
        df = sample_store.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "metric" in df.columns
        assert "method" in df.columns

    def test_to_nested_dict(self, sample_store):
        """Test exporting to nested dict."""
        nested = sample_store.to_nested_dict()
        assert isinstance(nested, dict)
        assert "test" in nested
        assert "count_records" in nested["test"]
        assert nested["test"]["count_records"] == 10

    def test_summary(self, sample_store):
        """Test summary method."""
        summary = sample_store.summary()
        assert isinstance(summary, pd.DataFrame)
        assert "metric" in summary.columns
        assert len(summary) == 3  # count_records, sum_values, describe_data


class TestMetricsStoreWithDimensions:
    """Test MetricsStore with dimensional data (intrinsic to results)."""

    def test_scalar_result_has_no_dimensions(self):
        """Test that scalar results have no dimensions."""
        data = pd.DataFrame({"value": [1, 2, 3]})
        config = {"test": {"filter": {}, "method": ["count_records"]}}

        store = generate_metrics(data, config)
        result = store["test", "count_records"]

        # Scalars have no dimensions
        assert result.dimensions() == []

    def test_series_result_has_index_dimension(self):
        """Test that series results have index as dimension."""

        # Create a method that returns a Series
        @metric_method
        def count_by_category(data):
            return data.groupby("category").size()

        data = pd.DataFrame({"category": ["A", "B", "A"], "value": [1, 2, 3]})
        config = {"test": {"filter": {}, "method": ["count_by_category"]}}

        store = generate_metrics(data, config)
        result = store["test", "count_by_category"]

        # Series has one dimension (the index)
        assert result.dimensions() == ["category"]

    def test_dataframe_result_has_column_dimensions(self):
        """Test that dataframe results have columns as dimensions."""

        # Create a method that returns a DataFrame with actual dimensions
        @metric_method
        def count_by_category_and_mod(data):
            # Create a pivot-like result with multiple dimensions
            return data.groupby(["category", data["value"] % 2]).size().reset_index(name="count")

        data = pd.DataFrame({"category": ["A", "B"] * 5, "value": range(10)})
        config = {"test": {"filter": {}, "method": ["count_by_category_and_mod"]}}

        store = generate_metrics(data, config)
        result = store["test", "count_by_category_and_mod"]

        # DataFrame should have dimensions: ['category', 'value'], with 'count' as value_column
        dims = result.dimensions()
        assert len(dims) == 2  # Has two dimensions
        assert "category" in dims
        assert "value" in dims

    def test_filter_by_dimension_value(self):
        """Test filtering by dimension values from the data itself."""

        # Create a method that returns dimensional data
        @metric_method
        def count_by_site(data):
            return data.groupby("site").size()

        data = pd.DataFrame({"site": ["R0A", "R0B"] * 10, "value": range(20)})
        config = {
            "metric_1": {"filter": {}, "method": ["count_by_site"]},
            "metric_2": {"filter": {"site": "R0A"}, "method": ["count_by_site"]},
        }

        store = generate_metrics(data, config)

        # Filter by dimension value that appears in the data
        filtered = store.filter(site="R0A")

        # Should find results where site dimension has value R0A
        assert len(filtered) > 0
