"""
Output format handling for Quick Metric results.

This module provides functionality to convert metric results between different
output formats while keeping complex data types (like DataFrames) intact.

The module follows the KISS principle by not over-flattening complex results
and maintaining the integrity of pandas DataFrames and Series objects.
"""

from enum import Enum
from typing import Union

import pandas as pd


class OutputFormat(Enum):
    """Supported output formats for generated metrics."""

    NESTED = "nested"  # Current dict of dicts: {'metric': {'method': result}}
    DATAFRAME = "dataframe"  # Long format DataFrame with metric/method/value columns
    FLAT_DATAFRAME = "flat_dataframe"  # Completely flat DataFrame (expands nested results)
    RECORDS = "records"  # List of dicts: [{'metric': '...', 'method': '...', 'value': ...}]


def convert_to_records(results: dict) -> list[dict]:
    """
    Convert nested results to records format.

    Keeps complex data types (DataFrames, Series) intact rather than flattening.

    Parameters
    ----------
    results : dict
        Nested results in format {'metric': {'method': result}}

    Returns
    -------
    List[dict]
        Records with structure [{'metric': str, 'method': str, 'value': Any}]
    """
    records = []

    for metric_name, metric_results in results.items():
        for method_name, result in metric_results.items():
            record = {
                "metric": metric_name,
                "method": method_name,
                "value": result,
                "value_type": type(result).__name__,
            }
            records.append(record)

    return records


def convert_to_dataframe(results: dict) -> pd.DataFrame:
    """
    Convert nested results to DataFrame format.

    Creates a long-format DataFrame while preserving complex data types.
    For DataFrames and Series, stores the object reference rather than flattening.

    Parameters
    ----------
    results : dict
        Nested results in format {'metric': {'method': result}}

    Returns
    -------
    pd.DataFrame
        Long format DataFrame with columns [metric, method, value, value_type]
    """
    records = convert_to_records(results)
    if not records:
        # Create empty DataFrame with expected columns
        return pd.DataFrame(columns=["metric", "method", "value", "value_type"])
    return pd.DataFrame(records)


def convert_to_flat_dataframe(results: dict) -> pd.DataFrame:
    """
    Convert nested results to a completely flat DataFrame format.

    This format creates one row per data point with clean, simple columns:
    - metric: metric name
    - method: method name
    - group_by: grouping variable(s) - single values or tuples for multiple groups
    - statistic: the statistic/column being measured
    - metric_value: the actual metric value

    Benefits of using tuples in group_by:
    - Preserves grouping structure instead of concatenating strings
    - Allows filtering by individual grouping levels:
      result[result['group_by'].apply(lambda x: x[0] == 'East')]
    - Maintains data types instead of converting everything to strings

    Parameters
    ----------
    results : dict
        Nested results dictionary

    Returns
    -------
    pd.DataFrame
        Flat DataFrame with one row per data point
    """
    dataframes_to_concat = []

    for metric_name, metric_results in results.items():
        for method_name, result in metric_results.items():
            if isinstance(result, pd.DataFrame):
                # Handle DataFrame results
                df_reset = result.reset_index()

                # Store original column structure for statistic tuples
                original_columns = result.columns
                col_mapping = None

                # Check if the original index was just a default RangeIndex
                has_meaningful_index = (
                    not isinstance(result.index, pd.RangeIndex) or result.index.name is not None
                )

                # For MultiIndex columns, create mapping before flattening
                if isinstance(original_columns, pd.MultiIndex):
                    # Create a mapping from flattened names to original tuples
                    col_mapping = {}
                    flattened_cols = []
                    for col in df_reset.columns:
                        if isinstance(col, tuple):
                            flattened = "_".join(str(i) for i in col if str(i) != "").strip("_")
                            col_mapping[flattened] = col
                            flattened_cols.append(flattened)
                        else:
                            col_mapping[col] = col
                            flattened_cols.append(col)

                    # Temporarily flatten for processing
                    df_reset.columns = flattened_cols

                # Find index columns (those added by reset_index)
                if isinstance(original_columns, pd.MultiIndex):
                    original_col_names = {
                        "_".join(str(i) for i in col if str(i) != "").strip("_")
                        for col in original_columns
                    }
                else:
                    original_col_names = set(original_columns)

                # Find which columns are index columns
                index_cols = [col for col in df_reset.columns if col not in original_col_names]

                # Handle the case where reset_index creates default 'index' column
                if not index_cols and "index" in df_reset.columns:
                    index_cols = ["index"]

                # Create group_by column using tuples to preserve structure
                if index_cols and has_meaningful_index:
                    if len(index_cols) == 1:
                        df_reset["group_by"] = df_reset[index_cols[0]]
                    else:
                        # Use tuples instead of concatenating strings
                        df_reset["group_by"] = df_reset[index_cols].apply(
                            lambda x: tuple(x), axis=1
                        )
                else:
                    # No meaningful grouping - use None instead of arbitrary values
                    df_reset["group_by"] = None

                # Melt the DataFrame
                value_cols = [
                    col for col in df_reset.columns if col not in index_cols + ["group_by"]
                ]

                df_melted = df_reset.melt(
                    id_vars=["group_by"],
                    value_vars=value_cols,
                    var_name="statistic",
                    value_name="metric_value",
                )

                # Convert statistic column to tuples if it came from MultiIndex
                if col_mapping is not None:
                    df_melted["statistic"] = df_melted["statistic"].map(
                        lambda x, mapping=col_mapping: mapping.get(x, x)
                    )

                # Add metadata
                df_melted["metric"] = metric_name
                df_melted["method"] = method_name

                # Reorder columns
                df_melted = df_melted[["metric", "method", "group_by", "statistic", "metric_value"]]
                dataframes_to_concat.append(df_melted)

            elif isinstance(result, pd.Series):
                # Handle Series results
                series_df = result.reset_index()

                # Handle MultiIndex series - use tuples for group_by
                if len(series_df.columns) > 2:
                    # Multiple index columns - use tuples instead of concatenation
                    index_cols = series_df.columns[:-1].tolist()  # All but the last (value) column
                    series_df["group_by"] = series_df[index_cols].apply(lambda x: tuple(x), axis=1)
                    # Keep only group_by and the value column
                    value_col = series_df.columns[-1]
                    series_df = series_df[["group_by", value_col]]
                    series_df.columns = ["group_by", "metric_value"]
                else:
                    # Single index column
                    series_df.columns = ["group_by", "metric_value"]

                series_df["metric"] = metric_name
                series_df["method"] = method_name
                series_df["statistic"] = result.name or "value"

                # No need to convert group_by to string - keep tuples or original types

                # Reorder columns
                series_df = series_df[["metric", "method", "group_by", "statistic", "metric_value"]]
                dataframes_to_concat.append(series_df)

            else:
                # Handle scalar values
                scalar_df = pd.DataFrame(
                    {
                        "metric": [metric_name],
                        "method": [method_name],
                        "group_by": [None],  # Use None for no grouping instead of 'all'
                        "statistic": ["value"],
                        "metric_value": [result],
                    }
                )
                dataframes_to_concat.append(scalar_df)

    if not dataframes_to_concat:
        return pd.DataFrame(columns=["metric", "method", "group_by", "statistic", "metric_value"])

    # Efficiently concatenate all DataFrames at once
    return pd.concat(dataframes_to_concat, ignore_index=True)


def convert_to_format(
    results: dict, output_format: OutputFormat
) -> Union[dict, pd.DataFrame, list[dict]]:
    """
    Convert nested results to the specified output format.

    Parameters
    ----------
    results : dict
        Nested results from interpret_metric_instructions
    output_format : OutputFormat
        Desired output format

    Returns
    -------
    Union[dict, pd.DataFrame, List[dict]]
        Results in the specified format

    Raises
    ------
    ValueError
        If output_format is not supported
    """
    if output_format == OutputFormat.NESTED:
        return results
    if output_format == OutputFormat.RECORDS:
        return convert_to_records(results)
    if output_format == OutputFormat.DATAFRAME:
        return convert_to_dataframe(results)
    if output_format == OutputFormat.FLAT_DATAFRAME:
        return convert_to_flat_dataframe(results)

    raise ValueError(f"Unsupported output format: {output_format}")


# Helper functions for format conversion
def to_dataframe(results: Union[dict, list[dict]]) -> pd.DataFrame:
    """
    Convert any results format to pandas DataFrame.

    Parameters
    ----------
    results : dict or List[dict]
        Results in nested or records format

    Returns
    -------
    pd.DataFrame
        Results as DataFrame
    """
    if isinstance(results, dict):
        return convert_to_dataframe(results)
    if isinstance(results, list):
        return pd.DataFrame(results)

    raise ValueError("Results must be dict (nested) or list (records) format")


def to_records(results: Union[dict, pd.DataFrame]) -> list[dict]:
    """
    Convert any results format to records format.

    Parameters
    ----------
    results : dict or pd.DataFrame
        Results in nested or dataframe format

    Returns
    -------
    List[dict]
        Results as list of dictionaries
    """
    if isinstance(results, dict):
        return convert_to_records(results)
    if isinstance(results, pd.DataFrame):
        return results.to_dict("records")

    raise ValueError("Results must be dict (nested) or DataFrame format")


def to_nested(results: Union[pd.DataFrame, list[dict]]) -> dict:
    """
    Convert results back to nested dictionary format.

    Parameters
    ----------
    results : pd.DataFrame or List[dict]
        Results in dataframe or records format

    Returns
    -------
    dict
        Results in nested format {'metric': {'method': value}}
    """
    if isinstance(results, list):
        # Convert from records
        nested = {}
        for record in results:
            metric = record["metric"]
            method = record["method"]
            value = record["value"]

            if metric not in nested:
                nested[metric] = {}
            nested[metric][method] = value
        return nested

    if isinstance(results, pd.DataFrame):
        # Convert from DataFrame
        nested = {}
        for _, row in results.iterrows():
            metric = row["metric"]
            method = row["method"]
            value = row["value"]

            if metric not in nested:
                nested[metric] = {}
            nested[metric][method] = value
        return nested

    raise ValueError("Results must be DataFrame or list (records) format")
