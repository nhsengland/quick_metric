"""
Tests for output format functionality.

Tests the various output format conversions to ensure they work correctly
with different types of metric results (scalars, DataFrames, Series).
"""

from typing import cast

import pandas as pd
import pytest

from quick_metric._output_formats import (
    OutputFormat,
    convert_to_dataframe,
    convert_to_flat_dataframe,
    convert_to_format,
    convert_to_records,
    to_dataframe,
    to_nested,
    to_records,
)


class TestOutputFormat:
    """Test OutputFormat enum."""

    @pytest.mark.parametrize(
        ("format_name", "expected_value"),
        [
            (OutputFormat.NESTED, "nested"),
            (OutputFormat.DATAFRAME, "dataframe"),
            (OutputFormat.RECORDS, "records"),
            (OutputFormat.FLAT_DATAFRAME, "flat_dataframe"),
        ],
    )
    def test_output_format_values(self, format_name, expected_value):
        """Test that OutputFormat has expected values."""
        assert format_name.value == expected_value


class TestConvertToRecords:
    """Test convert_to_records function."""

    def test_convert_scalar_results_structure(self):
        """Test that scalar results produce correct record structure."""
        results = {
            "metric1": {"method1": 42, "method2": 3.14},
            "metric2": {"method1": 100},
        }

        records = convert_to_records(results)

        assert len(records) == 3

    @pytest.mark.parametrize(
        ("metric", "method", "value", "expected_type"),
        [
            ("metric1", "method1", 42, "int"),
            ("metric1", "method2", 3.14, "float"),
            ("metric2", "method1", 100, "int"),
        ],
    )
    def test_convert_scalar_results_values(self, metric, method, value, expected_type):
        """Test that scalar results have correct values and types."""
        results = {
            "metric1": {"method1": 42, "method2": 3.14},
            "metric2": {"method1": 100},
        }

        records = convert_to_records(results)

        # Find the record we're testing
        record = next(r for r in records if r["metric"] == metric and r["method"] == method)

        assert record["value"] == value
        assert record["value_type"] == expected_type

    def test_convert_dataframe_results_count(self):
        """Test that DataFrame results produce single record."""
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        results = {"metric1": {"method1": df}}

        records = convert_to_records(results)

        assert len(records) == 1

    def test_convert_dataframe_results_structure(self):
        """Test that DataFrame results have correct structure."""
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        results = {"metric1": {"method1": df}}

        records = convert_to_records(results)
        record = records[0]

        assert record["metric"] == "metric1"
        assert record["method"] == "method1"

    def test_convert_dataframe_results_preservation(self):
        """Test that DataFrame is preserved in records."""
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        results = {"metric1": {"method1": df}}

        records = convert_to_records(results)
        record = records[0]

        assert isinstance(record["value"], pd.DataFrame)
        assert record["value_type"] == "DataFrame"

    def test_convert_series_results(self):
        """Test converting Series results to records format (keeping Series intact)."""
        series = pd.Series([1, 2, 3], name="test_series")
        results = {"metric1": {"method1": series}}

        records = convert_to_records(results)

        assert len(records) == 1
        assert records[0]["metric"] == "metric1"
        assert records[0]["method"] == "method1"
        assert isinstance(records[0]["value"], pd.Series)
        assert records[0]["value_type"] == "Series"


class TestConvertToDataframe:
    """Test convert_to_dataframe function."""

    @pytest.mark.parametrize(
        ("data", "expected_type"),
        [
            (42, "int"),
            (3.14, "float"),
            ("test", "str"),
            (pd.DataFrame({"a": [1]}), "DataFrame"),
            (pd.Series([1, 2, 3]), "Series"),
        ],
    )
    def test_single_metric_conversion(self, data, expected_type):
        """Test converting single metric with various data types."""
        results = {"metric1": {"method1": data}}

        df = convert_to_dataframe(results)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]["metric"] == "metric1"
        assert df.iloc[0]["method"] == "method1"
        assert df.iloc[0]["value_type"] == expected_type

    def test_dataframe_columns(self):
        """Test DataFrame has correct columns."""
        results = {"metric1": {"method1": 42}}

        df = convert_to_dataframe(results)

        assert list(df.columns) == ["metric", "method", "value", "value_type"]

    def test_multiple_metrics_count(self):
        """Test multiple metrics produce correct number of rows."""
        results = {
            "metric1": {"method1": 42, "method2": 3.14},
            "metric2": {"method1": 100},
        }

        df = convert_to_dataframe(results)

        assert len(df) == 3

    def test_scalar_value_preservation(self):
        """Test scalar values are preserved correctly."""
        results = {"metric1": {"method1": 42}}

        df = convert_to_dataframe(results)

        assert df.iloc[0]["value"] == 42

    def test_empty_results(self):
        """Test empty results produce empty DataFrame with correct structure."""
        results = {}

        df = convert_to_dataframe(results)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["metric", "method", "value", "value_type"]


class TestConvertToFormat:
    """Test convert_to_format function."""

    @pytest.mark.parametrize(
        ("format", "expected_type"),
        [
            (OutputFormat.NESTED, dict),
            (OutputFormat.RECORDS, list),
            (OutputFormat.DATAFRAME, pd.DataFrame),
            (OutputFormat.FLAT_DATAFRAME, pd.DataFrame),
        ],
    )
    def test_format_conversion_type(self, format, expected_type):
        """Test conversion to different formats returns correct type."""
        results = {"metric1": {"method1": 42}}

        converted = convert_to_format(results, format)

        assert isinstance(converted, expected_type)

    def test_nested_format_identity(self):
        """Test that nested format returns the same object."""
        results = {"metric1": {"method1": 42}}

        converted = convert_to_format(results, OutputFormat.NESTED)

        assert converted is results

    def test_records_format_structure(self):
        """Test records format has correct structure."""
        results = {"metric1": {"method1": 42}}

        records = convert_to_format(results, OutputFormat.RECORDS)

        assert len(records) == 1
        assert records[0]["value"] == 42

    def test_dataframe_format_structure(self):
        """Test DataFrame format has correct structure."""
        results = {"metric1": {"method1": 42}}

        df = convert_to_format(results, OutputFormat.DATAFRAME)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]["value"] == 42

    def test_flat_dataframe_format_structure(self):
        """Test flat DataFrame format has correct structure."""
        results = {"metric1": {"method1": 42}}

        df = convert_to_format(results, OutputFormat.FLAT_DATAFRAME)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        expected_columns = ["metric", "method", "group_by", "statistic", "metric_value"]
        assert list(df.columns) == expected_columns
        assert df.iloc[0]["metric_value"] == 42
        assert df.iloc[0]["group_by"] is None

    def test_invalid_format_raises_error(self):
        """Test that invalid format raises ValueError."""
        results = {"metric1": {"method1": 42}}

        with pytest.raises(ValueError, match="Unsupported output format"):
            convert_to_format(results, cast(OutputFormat, "invalid_format"))


class TestHelperFunctions:
    """Test helper functions for format conversion."""

    def test_to_dataframe_from_dict(self):
        """Test converting dict to DataFrame."""
        results = {"metric1": {"method1": 42}}

        df = to_dataframe(results)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_to_dataframe_from_list(self):
        """Test converting list to DataFrame."""
        records = [{"metric": "metric1", "method": "method1", "value": 42}]

        df = to_dataframe(records)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_to_records_from_dict(self):
        """Test converting dict to records."""
        results = {"metric1": {"method1": 42}}

        records = to_records(results)

        assert isinstance(records, list)
        assert len(records) == 1
        assert records[0]["value"] == 42

    def test_to_records_from_dataframe(self):
        """Test converting DataFrame to records."""
        df = pd.DataFrame([{"metric": "metric1", "method": "method1", "value": 42}])

        records = to_records(df)

        assert isinstance(records, list)
        assert len(records) == 1

    def test_to_nested_from_records(self):
        """Test converting records back to nested format."""
        records = [
            {"metric": "metric1", "method": "method1", "value": 42},
            {"metric": "metric1", "method": "method2", "value": 3.14},
        ]

        nested = to_nested(records)

        assert isinstance(nested, dict)
        assert "metric1" in nested
        assert nested["metric1"]["method1"] == 42
        assert nested["metric1"]["method2"] == 3.14

    def test_to_nested_from_dataframe(self):
        """Test converting DataFrame back to nested format."""
        df = pd.DataFrame(
            [
                {"metric": "metric1", "method": "method1", "value": 42},
                {"metric": "metric1", "method": "method2", "value": 3.14},
            ]
        )

        nested = to_nested(df)

        assert isinstance(nested, dict)
        assert "metric1" in nested
        assert nested["metric1"]["method1"] == 42
        assert nested["metric1"]["method2"] == 3.14

    def test_invalid_input_types_raise_errors(self):
        """Test that invalid input types raise appropriate errors."""
        with pytest.raises(ValueError, match="format"):
            to_dataframe(cast(dict, "invalid"))

        with pytest.raises(ValueError, match="format"):
            to_records(cast(dict, "invalid"))

        with pytest.raises(ValueError, match="format"):
            to_nested(cast(list, "invalid"))


class TestComplexDataTypes:
    """Test handling of complex data types like DataFrames and Series."""

    @pytest.mark.parametrize(
        ("data_type", "data"),
        [
            ("DataFrame", pd.DataFrame({"a": [1, 2], "b": [3, 4]})),
            ("Series", pd.Series([1, 2, 3], index=["a", "b", "c"])),
        ],
    )
    def test_complex_data_preserved(self, data_type, data):
        """Test that complex data types are preserved in records format."""
        results = {"metric1": {"method": data}}

        records = convert_to_records(results)

        assert len(records) == 1
        assert isinstance(records[0]["value"], type(data))
        assert records[0]["value_type"] == data_type

    def test_dataframe_equality(self):
        """Test DataFrame equality in converted records."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        results = {"metric1": {"df_method": df}}

        records = convert_to_records(results)

        pd.testing.assert_frame_equal(records[0]["value"], df)

    def test_series_equality(self):
        """Test Series equality in converted records."""
        series = pd.Series([1, 2, 3], index=["a", "b", "c"])
        results = {"metric1": {"series_method": series}}

        records = convert_to_records(results)

        pd.testing.assert_series_equal(records[0]["value"], series)

    def test_mixed_result_types_count(self):
        """Test correct number of records for mixed result types."""
        df = pd.DataFrame({"x": [1, 2]})
        series = pd.Series([10, 20])

        results = {
            "metric1": {
                "scalar_method": 42,
                "df_method": df,
                "series_method": series,
            }
        }

        records = convert_to_records(results)

        assert len(records) == 3

    def test_mixed_result_scalar_preservation(self):
        """Test scalar values are preserved correctly in mixed types."""
        df = pd.DataFrame({"x": [1, 2]})
        series = pd.Series([10, 20])

        results = {
            "metric1": {
                "scalar_method": 42,
                "df_method": df,
                "series_method": series,
            }
        }

        records = convert_to_records(results)
        scalar_record = next(r for r in records if r["method"] == "scalar_method")

        assert scalar_record["value"] == 42


class TestConvertToFlatDataframe:
    """Test convert_to_flat_dataframe function for the new flat format."""

    def test_flat_format_columns(self):
        """Test that flat DataFrame has the expected column structure."""
        results = {"metric1": {"method1": 42}}

        df = convert_to_flat_dataframe(results)

        expected_columns = ["metric", "method", "group_by", "statistic", "metric_value"]
        assert list(df.columns) == expected_columns

    @pytest.mark.parametrize(
        ("value", "expected_group_by", "expected_statistic"),
        [
            (42, None, "value"),
            (3.14, None, "value"),
            ("test", None, "value"),
        ],
    )
    def test_scalar_results_structure(self, value, expected_group_by, expected_statistic):
        """Test that scalar results produce correct flat structure with None group_by."""
        results = {"test_metric": {"scalar_method": value}}

        df = convert_to_flat_dataframe(results)

        assert len(df) == 1
        assert df.iloc[0]["metric"] == "test_metric"
        assert df.iloc[0]["method"] == "scalar_method"
        assert df.iloc[0]["group_by"] is expected_group_by
        assert df.iloc[0]["statistic"] == expected_statistic
        assert df.iloc[0]["metric_value"] == value

    def test_series_with_meaningful_index(self):
        """Test Series results preserve index values as group_by."""
        series = pd.Series({"total": 100, "mean": 25, "std": 10}, name="test_series")
        results = {"metric1": {"method1": series}}

        df = convert_to_flat_dataframe(results)

        assert len(df) == 3
        assert set(df["group_by"]) == {"total", "mean", "std"}
        assert all(df["statistic"] == "test_series")
        assert df[df["group_by"] == "total"]["metric_value"].iloc[0] == 100

    def test_dataframe_with_default_index_uses_none(self):
        """Test DataFrames with default RangeIndex use None for group_by."""
        summary_df = pd.DataFrame(
            {"total_sales": [1000], "avg_profit": [200], "max_revenue": [1500]}
        )
        results = {"summary": {"stats": summary_df}}

        df = convert_to_flat_dataframe(results)

        assert len(df) == 3
        assert all(df["group_by"].isna())  # All should be None
        assert set(df["statistic"]) == {"total_sales", "avg_profit", "max_revenue"}
        assert df[df["statistic"] == "total_sales"]["metric_value"].iloc[0] == 1000

    def test_dataframe_with_meaningful_grouping(self):
        """Test DataFrames with meaningful index preserve grouping."""
        grouped_df = pd.DataFrame({"sales": [100, 200], "profit": [20, 40]}, index=["A", "B"])
        grouped_df.index.name = "category"

        results = {"sales_analysis": {"by_category": grouped_df}}

        df = convert_to_flat_dataframe(results)

        assert len(df) == 4  # 2 groups × 2 statistics
        assert set(df["group_by"]) == {"A", "B"}
        assert set(df["statistic"]) == {"sales", "profit"}

        # Check specific values
        a_sales = df[(df["group_by"] == "A") & (df["statistic"] == "sales")]
        assert a_sales["metric_value"].iloc[0] == 100

    def test_multiindex_columns_preserve_tuples(self):
        """Test MultiIndex columns are preserved as tuples in statistic."""
        # Create DataFrame with MultiIndex columns
        df_multi = pd.DataFrame(
            {
                ("sales", "sum"): [300, 400],
                ("sales", "mean"): [150, 200],
                ("profit", "total"): [60, 80],
            },
            index=["North", "South"],
        )
        df_multi.index.name = "region"

        results = {"regional_analysis": {"multi_stats": df_multi}}

        df = convert_to_flat_dataframe(results)

        assert len(df) == 6  # 2 groups × 3 statistics
        assert set(df["group_by"]) == {"North", "South"}

        # Check that statistics are tuples
        statistics = set(df["statistic"])
        expected_stats = {("sales", "sum"), ("sales", "mean"), ("profit", "total")}
        assert statistics == expected_stats

        # Check specific value
        north_sales_sum = df[(df["group_by"] == "North") & (df["statistic"] == ("sales", "sum"))]
        assert north_sales_sum["metric_value"].iloc[0] == 300

    def test_multiple_grouping_levels_use_tuples(self):
        """Test multiple grouping levels are preserved as tuples."""
        # Create DataFrame with multiple index levels
        multi_index = pd.MultiIndex.from_tuples(
            [("North", "Electronics"), ("North", "Clothing"), ("South", "Electronics")],
            names=["region", "category"],
        )
        grouped_df = pd.DataFrame(
            {"sales": [500, 300, 400], "units": [50, 30, 40]}, index=multi_index
        )

        results = {"detailed_analysis": {"by_region_category": grouped_df}}

        df = convert_to_flat_dataframe(results)

        assert len(df) == 6  # 3 groups × 2 statistics

        # Check that group_by values are tuples
        group_by_values = set(df["group_by"])
        expected_groups = {
            ("North", "Electronics"),
            ("North", "Clothing"),
            ("South", "Electronics"),
        }
        assert group_by_values == expected_groups

        # Check specific value
        north_electronics_sales = df[
            (df["group_by"] == ("North", "Electronics")) & (df["statistic"] == "sales")
        ]
        assert north_electronics_sales["metric_value"].iloc[0] == 500

    def test_mixed_result_types(self):
        """Test handling multiple result types in same conversion."""
        # Mix of scalar, Series, and DataFrame
        series = pd.Series({"metric_a": 10, "metric_b": 20})
        df_grouped = pd.DataFrame({"value": [100, 200]}, index=["X", "Y"])

        results = {
            "scalars": {"total": 1000, "count": 50},
            "series_metrics": {"breakdown": series},
            "grouped_metrics": {"by_category": df_grouped},
        }

        df = convert_to_flat_dataframe(results)

        # Should have: 2 scalars + 2 series + 2 grouped = 6 rows
        assert len(df) == 6

        # Check scalars have None group_by
        scalar_rows = df[df["metric"].isin(["scalars"])]
        assert all(scalar_rows["group_by"].isna())

        # Check series preserves index
        series_rows = df[df["metric"] == "series_metrics"]
        assert set(series_rows["group_by"]) == {"metric_a", "metric_b"}

        # Check grouped preserves groups
        grouped_rows = df[df["metric"] == "grouped_metrics"]
        assert set(grouped_rows["group_by"]) == {"X", "Y"}

    @pytest.mark.parametrize(
        ("index_data", "should_preserve"),
        [
            (pd.RangeIndex(3), False),
            (pd.RangeIndex(3, name="row_num"), True),
            (pd.Index(["A", "B", "C"]), True),
            (pd.date_range("2024-01-01", periods=3), True),
        ],
    )
    def test_index_detection_logic(self, index_data, should_preserve):
        """Test the logic for detecting meaningful vs default indices."""
        df_test = pd.DataFrame({"value": [10, 20, 30]}, index=index_data)
        results = {"test": {"method": df_test}}

        df = convert_to_flat_dataframe(results)

        if should_preserve:
            # Should preserve the index values
            assert not all(df["group_by"].isna())
        else:
            # Should use None for meaningless default index
            assert all(df["group_by"].isna())

    def test_empty_results(self):
        """Test handling empty results dictionary."""
        results = {}

        df = convert_to_flat_dataframe(results)

        expected_columns = ["metric", "method", "group_by", "statistic", "metric_value"]
        assert list(df.columns) == expected_columns
        assert len(df) == 0

    def test_dataframe_filtering_capabilities(self):
        """Test that the flat format enables easy filtering operations."""
        # Create complex multi-level data
        multi_index = pd.MultiIndex.from_tuples(
            [("Q1", "North"), ("Q1", "South"), ("Q2", "North"), ("Q2", "South")],
            names=["quarter", "region"],
        )
        df_complex = pd.DataFrame(
            {
                ("revenue", "total"): [1000, 800, 1200, 900],
                ("revenue", "growth"): [0.1, 0.05, 0.15, 0.08],
                ("customers", "count"): [100, 80, 120, 90],
            },
            index=multi_index,
        )

        results = {"quarterly_analysis": {"multi_metric": df_complex}}
        df = convert_to_flat_dataframe(results)

        # Test filtering by first level of grouping (quarter)
        q1_data = df[df["group_by"].apply(lambda x: x[0] == "Q1" if x else False)]
        assert len(q1_data) == 6  # 2 regions × 3 metrics

        # Test filtering by statistic type
        revenue_data = df[
            df["statistic"].apply(lambda x: x[0] == "revenue" if isinstance(x, tuple) else False)
        ]
        assert len(revenue_data) == 8  # 4 groups × 2 revenue metrics

        # Test filtering by specific combination
        q2_north_revenue_total = df[
            (df["group_by"] == ("Q2", "North")) & (df["statistic"] == ("revenue", "total"))
        ]
        assert len(q2_north_revenue_total) == 1
        assert q2_north_revenue_total["metric_value"].iloc[0] == 1200
