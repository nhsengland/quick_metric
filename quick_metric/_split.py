"""
Data splitting functionality for quick_metric.

Handles splitting data by specified columns before metric calculation,
promoting results to higher dimensions (Scalar → Series → DataFrame).
"""

from typing import Optional, Union

from loguru import logger
import pandas as pd

from quick_metric._apply_methods import apply_methods
from quick_metric.exceptions import MetricSpecificationError
from quick_metric.results import DataFrameResult, ScalarResult, SeriesResult
from quick_metric.store import MetricsStore


def normalize_split_by(split_by) -> Optional[list[str]]:
    """
    Normalize split_by to a list of column names or None.

    Parameters
    ----------
    split_by : str | list[str] | None
        Split specification

    Returns
    -------
    list[str] | None
        Normalized list of column names, or None if no split

    Raises
    ------
    MetricSpecificationError
        If split_by is invalid type or contains non-strings
    """
    if split_by is None:
        return None
    if isinstance(split_by, str):
        return [split_by]
    if isinstance(split_by, list):
        if not all(isinstance(col, str) for col in split_by):
            raise MetricSpecificationError(
                f"split_by list must contain only strings, got: {split_by}",
                method_spec=split_by,
            )
        return split_by

    raise MetricSpecificationError(
        f"split_by must be str, list[str], or None, got {type(split_by)}: {split_by}",
        method_spec=split_by,
    )


def validate_split_columns(data: pd.DataFrame, split_by: list[str], metric_name: str) -> None:
    """
    Validate that split_by columns exist in the data.

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame to validate against
    split_by : list[str]
        Column names to validate
    metric_name : str
        Name of metric for error messages

    Raises
    ------
    MetricSpecificationError
        If any split_by columns are missing from data
    """
    missing = [col for col in split_by if col not in data.columns]
    if missing:
        raise MetricSpecificationError(
            f"Metric '{metric_name}': split_by columns not found in data: {missing}",
            method_spec=split_by,
        )


def process_with_split(
    data: pd.DataFrame,
    split_by: list[str],
    method_specs: list[Union[str, dict]],
    metrics_methods: dict,
    store: MetricsStore,
    metric_name: str,
) -> None:
    """
    Process metrics with data split by specified columns.

    Uses pandas groupby for efficiency. Each unique combination of split_by
    values becomes additional dimensions in the result:
    - ScalarResult → SeriesResult (single split) or DataFrameResult (multiple splits)
    - SeriesResult → DataFrameResult (splits become additional index levels/columns)
    - DataFrameResult → DataFrameResult (splits added as columns)

    Parameters
    ----------
    data : pd.DataFrame
        Filtered data to split and process
    split_by : list[str]
        Column names to split by (in order)
    method_specs : list[str | dict]
        Normalized method specifications
    metrics_methods : dict
        Available metric methods
    store : MetricsStore
        Store to add results to
    metric_name : str
        Name of the metric being processed

    Raises
    ------
    MetricSpecificationError
        If split_by columns don't exist in data
    """
    # Validate split columns exist
    validate_split_columns(data, split_by, metric_name)

    # Check if any split columns have all NaN - these won't create groups
    null_splits = [col for col in split_by if data[col].isna().all()]
    if null_splits:
        logger.warning(f"Split columns contain only NaN values: {null_splits}")

    # Use groupby for efficient splitting
    grouped = data.groupby(split_by, dropna=False, sort=True)
    n_groups = grouped.ngroups

    logger.debug(f"Created {n_groups} groups from {len(split_by)} split columns")

    # Collect results indexed by split values
    results_by_method = {}  # {method_name: [(split_key, result), ...]}

    for split_key, group_data in grouped:
        # split_key is a tuple for multiple splits, scalar for single split
        key_normalized = split_key if isinstance(split_key, tuple) else [split_key]
        split_dict = dict(zip(split_by, key_normalized))
        logger.trace(f"Processing split: {split_dict} ({len(group_data)} rows)")

        # Create temporary store for this group
        temp_store = MetricsStore()

        apply_methods(
            data=group_data,
            method_specs=method_specs,
            metrics_methods=metrics_methods,
            store=temp_store,
            metric_name=metric_name,
        )

        # Extract and store results
        for method_name in temp_store.methods(metric_name):
            result = temp_store[metric_name, method_name]

            if method_name not in results_by_method:
                results_by_method[method_name] = []

            results_by_method[method_name].append((split_key, result))

    # Combine results for each method
    for method_name, split_results in results_by_method.items():
        combined_result = combine_split_results(
            split_results=split_results,
            split_by=split_by,
            metric_name=metric_name,
            method_name=method_name,
        )

        store.add(combined_result)


def combine_split_results(
    split_results: list[tuple],
    split_by: list[str],
    metric_name: str,
    method_name: str,
) -> Union[ScalarResult, SeriesResult, DataFrameResult]:
    """
    Combine split results into a higher-dimensional result.

    Transformations:
    - ScalarResult + 1 split → SeriesResult (index = split_by)
    - ScalarResult + N splits → DataFrameResult (multi-index = split_by)
    - SeriesResult + splits → DataFrameResult (add split_by to index/columns)
    - DataFrameResult + splits → DataFrameResult (add split_by as columns)

    Parameters
    ----------
    split_results : list[tuple]
        List of (split_key, result) tuples from groupby
    split_by : list[str]
        Names of the split columns
    metric_name : str
        Name of the metric
    method_name : str
        Name of the method

    Returns
    -------
    MetricResult
        Combined result with split_by as additional dimensions
    """
    # Determine result type from first result
    _, first_result = split_results[0]

    if isinstance(first_result, ScalarResult):
        # Scalar → Series or DataFrame
        values = {}
        for split_key, result in split_results:
            # Normalize split_key to tuple
            key_tuple = split_key if isinstance(split_key, tuple) else (split_key,)
            values[key_tuple] = result.data

        if len(split_by) == 1:
            # Single split → Series
            series = pd.Series({k[0]: v for k, v in values.items()})
            series.index.name = split_by[0]
            return SeriesResult(metric=metric_name, method=method_name, data=series)

        # Multiple splits → DataFrame with MultiIndex
        index = pd.MultiIndex.from_tuples(values.keys(), names=split_by)
        series = pd.Series(list(values.values()), index=index, name="value")
        df = series.to_frame()
        return DataFrameResult(
            metric=metric_name, method=method_name, data=df, value_column="value"
        )

    if isinstance(first_result, SeriesResult):
        # Series → DataFrame
        # Each split becomes an additional index level or column
        dfs = []
        for split_key, result in split_results:
            df = result.data.to_frame(name="value")

            # Add split columns
            key_tuple = split_key if isinstance(split_key, tuple) else (split_key,)
            for col, val in zip(split_by, key_tuple):
                df[col] = val

            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=False)

        # Create hierarchical index: split_by + original index
        if combined_df.index.name:
            index_cols = split_by + [combined_df.index.name]
        else:
            index_cols = split_by + ["index"]

        combined_df = combined_df.reset_index()
        combined_df = combined_df.set_index(index_cols)

        return DataFrameResult(
            metric=metric_name, method=method_name, data=combined_df, value_column="value"
        )

    if isinstance(first_result, DataFrameResult):
        # DataFrame → DataFrame with additional split columns
        dfs = []
        for split_key, result in split_results:
            df = result.data.copy()

            # Add split columns
            key_tuple = split_key if isinstance(split_key, tuple) else (split_key,)
            for col, val in zip(split_by, key_tuple):
                df[col] = val

            dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)

        return DataFrameResult(
            metric=metric_name,
            method=method_name,
            data=combined_df,
            value_column=first_result.value_column,
        )

    raise MetricSpecificationError(
        f"Unknown result type: {type(first_result)}", method_spec=split_results
    )
