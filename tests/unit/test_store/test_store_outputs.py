"""Tests for MetricsStore output methods."""

import pandas as pd
import pytest

from quick_metric.results import DataFrameResult, ScalarResult, SeriesResult
from quick_metric.store import MetricsStore


class TestMetricsStoreOutputMethods:
    """Test output methods for MetricsStore."""

    @pytest.fixture
    def populated_store(self):
        store = MetricsStore()
        store.add(ScalarResult(metric="total", method="count", data=100))
        store.add(
            SeriesResult(
                metric="by_site", method="count", data=pd.Series([50, 50], index=["A", "B"])
            )
        )
        df = pd.DataFrame({"site": ["A", "B"], "category": ["X", "Y"], "value": [25, 25]})
        store.add(
            DataFrameResult(metric="detailed", method="analyze", data=df, value_column="value")
        )
        return store

    def test_to_records(self, populated_store):
        records = populated_store.to_records()

        assert isinstance(records, list)
        assert len(records) == 5  # 1 scalar + 2 series + 2 dataframe
        assert all(isinstance(r, dict) for r in records)
        assert all("metric" in r and "method" in r for r in records)

    def test_to_records_with_metadata(self, populated_store):
        records = populated_store.to_records(include_metadata=True)

        assert isinstance(records, list)
        assert all("value_type" in r for r in records)
        assert all("dimensions" in r for r in records)
        assert all("n_dimensions" in r for r in records)

    def test_to_nested_dict(self, populated_store):
        nested = populated_store.to_nested_dict()

        assert isinstance(nested, dict)
        assert "total" in nested
        assert "by_site" in nested
        assert "detailed" in nested
        assert nested["total"]["count"] == 100
        assert isinstance(nested["by_site"]["count"], pd.Series)

    def test_to_nested_dict_with_metadata(self, populated_store):
        nested = populated_store.to_nested_dict(include_metadata=True)

        assert isinstance(nested, dict)
        total_result = nested["total"]["count"]
        assert "data" in total_result
        assert "value_type" in total_result
        assert "dimensions" in total_result
        assert total_result["value_type"] == "scalar"

    def test_to_dataframes(self, populated_store):
        dfs = populated_store.to_dataframes()

        assert isinstance(dfs, dict)
        assert "_scalars" in dfs
        assert "by_site" in dfs
        assert "detailed" in dfs
        assert isinstance(dfs["_scalars"], pd.DataFrame)
        assert len(dfs["_scalars"]) == 1

    def test_to_dataframes_no_separate_scalars(self, populated_store):
        dfs = populated_store.to_dataframes(separate_scalars=False)

        assert isinstance(dfs, dict)
        assert "total" in dfs
        assert "_scalars" not in dfs

    def test_to_dict_of_series(self, populated_store):
        series_dict = populated_store.to_dict_of_series()

        assert isinstance(series_dict, dict)
        assert ("total", "count") in series_dict
        assert ("by_site", "count") in series_dict
        assert ("detailed", "analyze") in series_dict
        assert series_dict[("total", "count")] == 100

    def test_to_dict_by_metric(self, populated_store):
        by_metric = populated_store.to_dict_by_metric()

        assert isinstance(by_metric, dict)
        assert "total" in by_metric
        assert "count" in by_metric["total"]
        # Should be same as to_nested_dict
        assert by_metric == populated_store.to_nested_dict()

    def test_to_dict_by_method(self, populated_store):
        by_method = populated_store.to_dict_by_method()

        assert isinstance(by_method, dict)
        assert "count" in by_method
        assert "analyze" in by_method
        assert "total" in by_method["count"]
        assert "by_site" in by_method["count"]

    def test_to_datasets(self, populated_store):
        datasets = populated_store.to_datasets()

        assert isinstance(datasets, dict)
        assert "total" in datasets
        assert "by_site" in datasets
        assert "detailed" in datasets
        # All should be DataFrames
        assert all(isinstance(v, dict) for v in datasets.values())
        assert all(isinstance(df, pd.DataFrame) for d in datasets.values() for df in d.values())

    def test_to_datasets_converts_scalar_to_dataframe(self, populated_store):
        datasets = populated_store.to_datasets()
        scalar_df = datasets["total"]["count"]

        assert isinstance(scalar_df, pd.DataFrame)
        assert "value" in scalar_df.columns
        assert len(scalar_df) == 1
        assert scalar_df.iloc[0]["value"] == 100

    def test_to_datasets_converts_series_to_dataframe(self, populated_store):
        datasets = populated_store.to_datasets()
        series_df = datasets["by_site"]["count"]

        assert isinstance(series_df, pd.DataFrame)
        assert "value" in series_df.columns
        assert len(series_df) == 2


class TestMetricsStoreOutputWithMixedDimensions:
    """Test output methods handle varying dimensionality correctly."""

    @pytest.fixture
    def mixed_store(self):
        store = MetricsStore()
        # 0D - scalar
        store.add(ScalarResult(metric="total", method="overall", data=1000))
        # 1D - series
        store.add(
            SeriesResult(
                metric="by_site", method="count", data=pd.Series([500, 500], index=["A", "B"])
            )
        )
        # 2D - dataframe
        df = pd.DataFrame(
            {
                "site": ["A", "A", "B", "B"],
                "month": ["Jan", "Feb", "Jan", "Feb"],
                "count": [100, 150, 120, 130],
            }
        )
        store.add(
            DataFrameResult(metric="by_site_month", method="cross", data=df, value_column="count")
        )
        return store

    def test_to_dataframe_handles_nan_for_missing_dimensions(self, mixed_store):
        df = mixed_store.to_dataframe()

        assert isinstance(df, pd.DataFrame)
        # Should have columns for all dimensions plus metric/method/value
        assert "metric" in df.columns
        assert "method" in df.columns
        # Scalar row should have NaN for site/month
        # Avoid boolean indexing due to pandas/numpy compatibility issues - iterate instead
        scalar_row = None
        for _idx, row in df.iterrows():
            if row["metric"] == "total":
                scalar_row = row
                break
        assert scalar_row is not None
        assert pd.isna(scalar_row.get("index"))
        assert pd.isna(scalar_row.get("site"))
        assert pd.isna(scalar_row.get("month"))

    def test_to_dataframes_keeps_dimensions_separate(self, mixed_store):
        dfs = mixed_store.to_dataframes()

        # Each metric should have only its relevant dimensions
        assert "_scalars" in dfs
        assert "value" in dfs["_scalars"].columns
        assert "index" not in dfs["_scalars"].columns

        assert "by_site" in dfs
        assert "index" in dfs["by_site"].columns

        assert "by_site_month" in dfs
        assert "site" in dfs["by_site_month"].columns
        assert "month" in dfs["by_site_month"].columns
