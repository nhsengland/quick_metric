"""Tests for MetricResult output methods (to_dataframe, to_dict, to_series, repr)."""

import pandas as pd
import pytest

from quick_metric.results import DataFrameResult, ScalarResult, SeriesResult


class TestScalarResultOutputMethods:
    """Test output methods for ScalarResult."""

    def test_to_dataframe(self):
        result = ScalarResult(metric="total", method="count", data=100)
        df = result.to_dataframe()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert list(df.columns) == ["metric", "method", "value"]
        assert df.iloc[0]["value"] == 100

    def test_to_dict(self):
        result = ScalarResult(metric="total", method="count", data=100)
        assert result.to_dict() == 100

    def test_to_dict_with_metadata(self):
        result = ScalarResult(metric="total", method="count", data=100)
        meta = result.to_dict(include_metadata=True)

        assert meta["metric"] == "total"
        assert meta["method"] == "count"
        assert meta["value_type"] == "scalar"
        assert meta["dimensions"] == []
        assert meta["data"] == 100

    def test_to_series(self):
        result = ScalarResult(metric="total", method="count", data=100)
        series = result.to_series()

        assert isinstance(series, pd.Series)
        assert len(series) == 1
        assert series.iloc[0] == 100
        assert series.name == "total_count"

    def test_repr(self):
        result = ScalarResult(metric="total", method="count", data=100)
        repr_str = repr(result)

        assert "ScalarResult" in repr_str
        assert "total" in repr_str
        assert "count" in repr_str

    def test_str(self):
        result = ScalarResult(metric="total", method="count", data=100)
        str_repr = str(result)

        assert "total.count" in str_repr
        assert "scalar" in str_repr


class TestSeriesResultOutputMethods:
    """Test output methods for SeriesResult."""

    def test_to_dataframe(self):
        series = pd.Series([10, 20, 30], index=["A", "B", "C"])
        result = SeriesResult(metric="by_cat", method="count", data=series)
        df = result.to_dataframe()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "metric" in df.columns
        assert "method" in df.columns
        assert "index" in df.columns
        assert "value" in df.columns

    def test_to_dict(self):
        series = pd.Series([10, 20], index=["A", "B"])
        result = SeriesResult(metric="by_cat", method="count", data=series)
        returned = result.to_dict()

        assert isinstance(returned, pd.Series)
        assert returned.equals(series)

    def test_to_dict_with_metadata(self):
        series = pd.Series([10, 20], index=["A", "B"])
        result = SeriesResult(metric="by_cat", method="count", data=series)
        meta = result.to_dict(include_metadata=True)

        assert meta["metric"] == "by_cat"
        assert meta["method"] == "count"
        assert meta["value_type"] == "series"
        assert meta["dimensions"] == ["index"]
        assert isinstance(meta["data"], pd.Series)

    def test_to_series(self):
        series = pd.Series([10, 20], index=["A", "B"])
        result = SeriesResult(metric="by_cat", method="count", data=series)
        returned = result.to_series()

        assert isinstance(returned, pd.Series)
        assert returned.equals(series)


class TestDataFrameResultOutputMethods:
    """Test output methods for DataFrameResult."""

    def test_to_dataframe(self):
        df = pd.DataFrame({"site": ["A", "B"], "count": [10, 20]})
        result = DataFrameResult(metric="by_site", method="analyze", data=df, value_column="count")
        output_df = result.to_dataframe()

        assert isinstance(output_df, pd.DataFrame)
        assert "metric" in output_df.columns
        assert "method" in output_df.columns
        assert "site" in output_df.columns
        assert "count" in output_df.columns

    def test_to_dict(self):
        df = pd.DataFrame({"site": ["A", "B"], "count": [10, 20]})
        result = DataFrameResult(metric="by_site", method="analyze", data=df, value_column="count")
        returned = result.to_dict()

        assert isinstance(returned, pd.DataFrame)
        # Avoid .equals() due to pandas/numpy compatibility issues
        assert list(returned.columns) == list(df.columns)
        assert len(returned) == len(df)
        assert returned["site"].tolist() == df["site"].tolist()
        assert returned["count"].tolist() == df["count"].tolist()

    def test_to_series_single_column(self):
        df = pd.DataFrame({"count": [10, 20, 30]})
        result = DataFrameResult(metric="values", method="get", data=df, value_column="count")
        series = result.to_series()

        assert isinstance(series, pd.Series)
        assert len(series) == 3

    def test_to_series_multi_column_raises_error(self):
        df = pd.DataFrame({"site": ["A", "B"], "count": [10, 20]})
        result = DataFrameResult(metric="by_site", method="analyze", data=df, value_column="count")

        with pytest.raises(ValueError, match="Cannot convert DataFrame with 2 columns"):
            result.to_series()

    def test_get_column(self):
        df = pd.DataFrame({"site": ["A", "B"], "count": [10, 20]})
        result = DataFrameResult(metric="by_site", method="analyze", data=df, value_column="count")
        site_col = result.get_column("site")

        assert isinstance(site_col, pd.Series)
        assert list(site_col) == ["A", "B"]

    def test_get_column_not_found(self):
        df = pd.DataFrame({"site": ["A", "B"], "count": [10, 20]})
        result = DataFrameResult(metric="by_site", method="analyze", data=df, value_column="count")

        with pytest.raises(KeyError, match="Column 'nonexistent' not found"):
            result.get_column("nonexistent")


class TestJupyterReprHtml:
    """Test Jupyter notebook HTML representation."""

    def test_scalar_result_repr_html(self):
        result = ScalarResult(metric="total", method="count", data=100)
        html = result._repr_html_()

        assert isinstance(html, str)
        assert "total.count" in html
        assert "scalar" in html
        assert "<" in html  # Has HTML tags

    def test_series_result_repr_html(self):
        series = pd.Series([10, 20], index=["A", "B"])
        result = SeriesResult(metric="by_cat", method="count", data=series)
        html = result._repr_html_()

        assert isinstance(html, str)
        assert "by_cat.count" in html
        assert "series" in html

    def test_dataframe_result_repr_html(self):
        df = pd.DataFrame({"site": ["A", "B"], "count": [10, 20]})
        result = DataFrameResult(metric="by_site", method="analyze", data=df, value_column="count")
        html = result._repr_html_()

        assert isinstance(html, str)
        assert "by_site.analyze" in html
        assert "dataframe" in html
