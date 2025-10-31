"""Test create_result integration with different data types and formats."""

import pandas as pd
import pytest

from quick_metric.exceptions import MetricSpecificationError
from quick_metric.results import (
    DataFrameResult,
    ScalarResult,
    SeriesResult,
    create_result,
)
from quick_metric.store import MetricsStore


class TestCreateResultDirectData:
    """Test create_result with direct data types."""

    def test_scalar_result(self):
        """Test creating scalar result from direct value."""
        result = create_result("metric1", "method1", 100)
        assert isinstance(result, ScalarResult)
        assert result.data == 100
        assert result.dimensions() == []

    def test_series_result(self):
        """Test creating series result from pd.Series."""
        series = pd.Series([10, 20, 30], index=["A", "B", "C"])
        result = create_result("metric1", "method1", series)
        assert isinstance(result, SeriesResult)
        assert len(result.data) == 3
        assert result.dimensions() == ["index"]

    def test_dataframe_result(self):
        """Test creating dataframe result from pd.DataFrame."""
        df = pd.DataFrame({"cat": ["A", "B"], "count": [10, 20]})
        result = create_result("metric1", "method1", df, value_column="count")
        assert isinstance(result, DataFrameResult)
        assert result.value_column == "count"
        assert result.dimensions() == ["cat"]


class TestCreateResultDictFormat:
    """Test create_result with dict-based return formats."""

    def test_dict_with_data_key(self):
        """Test dict with 'data' key extracts data correctly."""
        df = pd.DataFrame({"cat": ["A", "B"], "count": [10, 20]})
        result = create_result(
            "metric1",
            "method1",
            {"data": df, "value_column": "count"},
        )
        assert isinstance(result, DataFrameResult)
        assert result.value_column == "count"

    def test_dict_with_value_key(self):
        """Test dict with 'value' key creates scalar result."""
        result = create_result("metric1", "method1", {"value": 42})
        assert isinstance(result, ScalarResult)
        assert result.data == 42

    def test_dict_value_column_priority(self):
        """Test that dict value_column takes priority over parameter."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        result = create_result(
            "metric1",
            "method1",
            {"data": df, "value_column": "b"},
            value_column="c",  # This should be overridden
        )
        assert result.value_column == "b"
        assert "a" in result.dimensions()
        assert "c" in result.dimensions()
        assert "b" not in result.dimensions()

    def test_dict_without_recognized_keys_raises_error(self):
        """Test that dict without 'data' or 'value' keys raises error."""
        with pytest.raises(MetricSpecificationError) as exc_info:
            create_result("metric1", "method1", {"foo": "bar", "baz": 123})

        error_msg = str(exc_info.value)
        assert "without 'data' or 'value' keys" in error_msg
        assert "foo" in error_msg
        assert "baz" in error_msg


class TestCreateResultAlreadyResult:
    """Test create_result when passed an existing MetricResult."""

    def test_returns_same_instance(self):
        """Test that passing a MetricResult returns it unchanged."""
        original = ScalarResult("metric1", "method1", 100)
        result = create_result("metric2", "method2", original)
        assert result is original
        assert result.metric == "metric1"  # Original values preserved
        assert result.method == "method1"


class TestMetricsStoreAddFromMethod:
    """Test MetricsStore.add_from_method convenience method."""

    def test_add_from_method_scalar(self):
        """Test adding scalar via add_from_method."""
        store = MetricsStore()
        result = store.add_from_method("metric1", "count", 100)

        assert isinstance(result, ScalarResult)
        assert len(store) == 1
        assert store.value("metric1", "count") == 100

    def test_add_from_method_series(self):
        """Test adding series via add_from_method."""
        store = MetricsStore()
        series = pd.Series([10, 20], index=["A", "B"])
        result = store.add_from_method("metric1", "by_cat", series)

        assert isinstance(result, SeriesResult)
        assert len(store) == 1

    def test_add_from_method_dict(self):
        """Test adding via dict with add_from_method."""
        store = MetricsStore()
        df = pd.DataFrame({"cat": ["A", "B"], "count": [10, 20]})
        result = store.add_from_method("metric1", "analyze", {"data": df, "value_column": "count"})

        assert isinstance(result, DataFrameResult)
        assert len(store) == 1
        assert result.value_column == "count"

    def test_add_from_method_multiple(self):
        """Test adding multiple results."""
        store = MetricsStore()
        store.add_from_method("m1", "count", 100)
        store.add_from_method("m1", "sum", 500)
        store.add_from_method("m2", "count", 50)

        assert len(store) == 3
        assert store.value("m1", "count") == 100
        assert store.value("m2", "count") == 50

    def test_add_from_method_indexes_series_dimensions(self):
        """Test that Series dimensions are indexed for filtering."""
        store = MetricsStore()
        series = pd.Series([10, 20, 30], index=["A", "B", "C"])
        series.index.name = "category"
        store.add_from_method("metric1", "by_cat", series)

        # Verify dimensions are indexed
        assert len(store._dimension_index) > 0

        # Can filter by dimension values
        filtered = store.filter(category="A")
        assert len(filtered) == 1
        assert ("metric1", "by_cat") in list(filtered.keys())

    def test_add_from_method_indexes_dataframe_dimensions(self):
        """Test that DataFrame dimensions are indexed for filtering."""
        store = MetricsStore()
        df = pd.DataFrame(
            {"site": ["R0A", "R0B", "R0A"], "month": ["Jan", "Feb", "Jan"], "count": [50, 75, 60]}
        )
        store.add_from_method("metric1", "by_site_month", df, value_column="count")

        # Verify dimensions are indexed
        assert len(store._dimension_index) > 0

        # Can filter by dimension values
        filtered_site = store.filter(site="R0A")
        assert len(filtered_site) == 1

        filtered_month = store.filter(month="Jan")
        assert len(filtered_month) == 1

    def test_add_from_method_dict_indexes_dimensions(self):
        """Test that dict-based DataFrame results index dimensions."""
        store = MetricsStore()
        df = pd.DataFrame(
            {
                "category": ["A", "B", "A"],
                "status": ["active", "active", "inactive"],
                "count": [10, 20, 5],
            }
        )
        store.add_from_method("metric1", "analyze", {"data": df, "value_column": "count"})

        # Verify all dimension values are indexed
        assert len(store._dimension_index) > 0

        # Can filter by any dimension
        filtered_cat = store.filter(category="A")
        assert len(filtered_cat) == 1

        filtered_status = store.filter(status="active")
        assert len(filtered_status) == 1


class TestCreateResultLogging:
    """Test that appropriate log messages are generated.

    Note: These tests verify behavior without checking logs since loguru
    uses a different logging mechanism than standard Python logging.
    Log messages can be verified manually or with loguru-specific testing.
    """

    def test_dataframe_without_value_column_uses_last_column(self):
        """Test that DataFrame without value_column uses last column."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = create_result("metric1", "method1", df)

        assert isinstance(result, DataFrameResult)
        assert result.value_column == "b"  # Last column used
        assert result.dimensions() == ["a"]

    def test_dict_with_value_creates_scalar(self):
        """Test that dict with 'value' key creates scalar result."""
        result = create_result("metric1", "method1", {"value": 42})

        assert result.data == 42
        assert isinstance(result, ScalarResult)

    def test_already_result_returns_unchanged(self):
        """Test that passing MetricResult returns it unchanged."""
        original = ScalarResult("m1", "meth1", 100)
        result = create_result("m2", "meth2", original)

        assert result is original
        assert result.metric == "m1"  # Original preserved
