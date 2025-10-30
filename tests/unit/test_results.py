"""Test MetricResult classes and their methods."""

import pandas as pd

from quick_metric.results import (
    DataFrameResult,
    ScalarResult,
    SeriesResult,
)


class TestMetricResultMatches:
    """Test the matches() method on all MetricResult types."""

    def test_scalar_matches_metric(self):
        """Test matching on metric name for scalar result."""
        result = ScalarResult("sales_total", "sum", 1000)

        assert result.matches(metric="sales_total")
        assert not result.matches(metric="other_metric")

    def test_scalar_matches_method(self):
        """Test matching on method name for scalar result."""
        result = ScalarResult("sales_total", "sum", 1000)

        assert result.matches(method="sum")
        assert not result.matches(method="count")

    def test_scalar_matches_value_type(self):
        """Test matching on value_type for scalar result."""
        result = ScalarResult("sales_total", "sum", 1000)

        assert result.matches(value_type="scalar")
        assert not result.matches(value_type="series")
        assert not result.matches(value_type="dataframe")

    def test_scalar_matches_multiple_filters(self):
        """Test matching on multiple filters simultaneously."""
        result = ScalarResult("sales_total", "sum", 1000)

        assert result.matches(metric="sales_total", method="sum")
        assert result.matches(metric="sales_total", value_type="scalar")
        assert not result.matches(metric="sales_total", method="count")

    def test_series_matches_metric(self):
        """Test matching on metric name for series result."""
        series = pd.Series([10, 20, 30], index=["A", "B", "C"])
        result = SeriesResult("category_counts", "count_by_category", series)

        assert result.matches(metric="category_counts")
        assert not result.matches(metric="other_metric")

    def test_series_matches_value_type(self):
        """Test matching on value_type for series result."""
        series = pd.Series([10, 20, 30], index=["A", "B", "C"])
        result = SeriesResult("category_counts", "count_by_category", series)

        assert result.matches(value_type="series")
        assert not result.matches(value_type="scalar")

    def test_series_matches_dimension_value(self):
        """Test matching on dimension values for series result."""
        series = pd.Series([10, 20, 30], index=["A", "B", "C"])
        result = SeriesResult("category_counts", "count_by_category", series)

        # Should match if any record has the dimension value
        assert result.matches(index="A")
        assert result.matches(index="B")
        assert not result.matches(index="D")

    def test_series_matches_with_named_index(self):
        """Test matching on named index dimension."""
        series = pd.Series([10, 20, 30], index=pd.Index(["A", "B", "C"], name="category"))
        result = SeriesResult("category_counts", "count_by_category", series)

        assert result.matches(category="A")
        assert not result.matches(category="D")

    def test_dataframe_matches_metric(self):
        """Test matching on metric name for dataframe result."""
        df = pd.DataFrame({"region": ["North", "South"], "sales": [100, 200]})
        result = DataFrameResult("regional_sales", "sum_by_region", df, value_column="sales")

        assert result.matches(metric="regional_sales")
        assert not result.matches(metric="other_metric")

    def test_dataframe_matches_value_type(self):
        """Test matching on value_type for dataframe result."""
        df = pd.DataFrame({"region": ["North", "South"], "sales": [100, 200]})
        result = DataFrameResult("regional_sales", "sum_by_region", df, value_column="sales")

        assert result.matches(value_type="dataframe")
        assert not result.matches(value_type="scalar")

    def test_dataframe_matches_dimension_value(self):
        """Test matching on dimension values for dataframe result."""
        df = pd.DataFrame({"region": ["North", "South"], "sales": [100, 200]})
        result = DataFrameResult("regional_sales", "sum_by_region", df, value_column="sales")

        assert result.matches(region="North")
        assert result.matches(region="South")
        assert not result.matches(region="East")

    def test_dataframe_matches_multiple_dimensions(self):
        """Test matching on multiple dimension values."""
        df = pd.DataFrame(
            {
                "region": ["North", "South", "North"],
                "product": ["A", "A", "B"],
                "sales": [100, 200, 150],
            }
        )
        result = DataFrameResult("sales", "sum", df, value_column="sales")

        assert result.matches(region="North")
        assert result.matches(product="A")
        assert result.matches(region="North", product="B")
        assert not result.matches(region="East")

    def test_matches_with_no_filters(self):
        """Test that matches() returns True when no filters provided."""
        result = ScalarResult("metric1", "method1", 100)
        assert result.matches()

    def test_matches_dimension_not_in_special_keys(self):
        """Test that dimension filtering works correctly."""
        df = pd.DataFrame({"category": ["A", "B"], "count": [10, 20]})
        result = DataFrameResult("metric1", "method1", df, value_column="count")

        # Test matching on actual dimension
        assert result.matches(category="A")
        assert not result.matches(category="C")


class TestSeriesDimensions:
    """Test dimensions() method for SeriesResult."""

    def test_series_dimensions_unnamed_index(self):
        """Test dimensions with unnamed index."""
        series = pd.Series([10, 20, 30], index=["A", "B", "C"])
        result = SeriesResult("metric1", "method1", series)

        assert result.dimensions() == ["index"]

    def test_series_dimensions_named_index(self):
        """Test dimensions with named index."""
        series = pd.Series([10, 20, 30], index=pd.Index(["A", "B", "C"], name="category"))
        result = SeriesResult("metric1", "method1", series)

        assert result.dimensions() == ["category"]


class TestDataFrameDimensions:
    """Test dimensions() method for DataFrameResult."""

    def test_dataframe_dimensions_single_dimension(self):
        """Test dimensions with one dimension column."""
        df = pd.DataFrame({"region": ["North", "South"], "sales": [100, 200]})
        result = DataFrameResult("metric1", "method1", df, value_column="sales")

        assert result.dimensions() == ["region"]

    def test_dataframe_dimensions_multiple_dimensions(self):
        """Test dimensions with multiple dimension columns."""
        df = pd.DataFrame(
            {"region": ["North", "South"], "product": ["A", "B"], "sales": [100, 200]}
        )
        result = DataFrameResult("metric1", "method1", df, value_column="sales")

        dimensions = result.dimensions()
        assert set(dimensions) == {"region", "product"}

    def test_dataframe_dimensions_empty_dataframe(self):
        """Test dimensions with empty dataframe."""
        df = pd.DataFrame()
        result = DataFrameResult("metric1", "method1", df, value_column="value")

        assert result.dimensions() == []


class TestDataFrameToRecords:
    """Test to_records() method for DataFrameResult."""

    def test_dataframe_to_records_basic(self):
        """Test converting dataframe to records."""
        df = pd.DataFrame({"region": ["North", "South"], "sales": [100, 200]})
        result = DataFrameResult("regional_sales", "sum", df, value_column="sales")

        records = result.to_records()

        assert len(records) == 2
        assert records[0] == {
            "metric": "regional_sales",
            "method": "sum",
            "region": "North",
            "sales": 100,  # Value column keeps its original name
        }
        assert records[1] == {
            "metric": "regional_sales",
            "method": "sum",
            "region": "South",
            "sales": 200,
        }

    def test_dataframe_to_records_multiple_dimensions(self):
        """Test converting dataframe with multiple dimensions to records."""
        df = pd.DataFrame(
            {"region": ["North", "South"], "product": ["A", "B"], "sales": [100, 200]}
        )
        result = DataFrameResult("sales", "sum", df, value_column="sales")

        records = result.to_records()

        assert len(records) == 2
        assert "region" in records[0]
        assert "product" in records[0]
        assert "sales" in records[0]  # Value column keeps its original name
        assert records[0]["sales"] == 100


class TestSeriesValueType:
    """Test value_type() method for SeriesResult."""

    def test_series_value_type(self):
        """Test that SeriesResult returns 'series' as value_type."""
        series = pd.Series([10, 20, 30])
        result = SeriesResult("metric1", "method1", series)

        assert result.value_type() == "series"


class TestDataFrameValueType:
    """Test value_type() method for DataFrameResult."""

    def test_dataframe_value_type(self):
        """Test that DataFrameResult returns 'dataframe' as value_type."""
        df = pd.DataFrame({"col": [1, 2]})
        result = DataFrameResult("metric1", "method1", df, value_column="col")

        assert result.value_type() == "dataframe"
