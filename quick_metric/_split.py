"""
Data splitting functionality for quick_metric.

Handles splitting data by specified columns before metric calculation,
promoting results to higher dimensions (Scalar → Series → DataFrame).
"""

import hashlib
from typing import Optional, Union

from dask.base import tokenize as dask_tokenize
import dask.dataframe as dd
from loguru import logger
import pandas as pd

from quick_metric.exceptions import MetricsMethodNotFoundError, MetricSpecificationError
from quick_metric.results import DataFrameResult, SeriesResult
from quick_metric.store import MetricsStore


class _MethodApplier:
    """Callable wrapper for applying methods in groupby operations.

    This class is serializable by Dask and provides deterministic hashing.
    """

    def __init__(self, method: callable, params: Optional[dict] = None):
        """Initialize the method applier.

        Parameters
        ----------
        method : callable
            The metric method to apply
        params : dict, optional
            Parameters to pass to the method
        """
        self.method = method
        self.params = params or {}
        # Store method name for deterministic hashing
        self.method_name = getattr(method, "__name__", repr(method))

    def __call__(self, df):
        """Apply the method to the DataFrame."""
        if self.params:
            return self.method(df, **self.params)
        return self.method(df)

    def __reduce__(self):
        """Support pickling for Dask serialization."""
        return (_MethodApplier, (self.method, self.params))

    def __hash__(self):
        """Provide deterministic hashing."""
        param_str = str(sorted(self.params.items())) if self.params else ""
        return hash((self.method_name, param_str))

    def __eq__(self, other):
        """Support equality comparison."""
        if not isinstance(other, _MethodApplier):
            return False
        return self.method_name == other.method_name and self.params == other.params

    def __dask_tokenize__(self):
        """Provide deterministic tokenization for Dask."""
        param_str = str(sorted(self.params.items())) if self.params else ""
        return dask_tokenize(self.method_name, param_str)


def normalize_split_by(split_by: Union[str, list[str], None]) -> Optional[list[str]]:
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


def validate_split_columns(data: dd.DataFrame, split_by: list[str], metric_name: str) -> None:
    """
    Validate that split_by columns exist in the data.

    Parameters
    ----------
    data : dd.DataFrame
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
    data: dd.DataFrame,
    split_by: list[str],
    method_specs: list[Union[str, dict]],
    metrics_methods: dict,
    store: MetricsStore,
    metric_name: str,
) -> None:
    """
    Process metrics with data split by specified columns using Dask.

    Uses Dask's groupby.apply() to process groups in parallel without
    materializing all groups at once. Each unique combination of split_by
    values becomes additional dimensions in the result.

    Parameters
    ----------
    data : dd.DataFrame
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

    logger.debug(f"Processing with split_by: {split_by}")

    # For each method, apply it using groupby.apply() which stays lazy
    results_by_method = {}

    for method_spec in method_specs:
        # Parse method spec
        if isinstance(method_spec, str):
            method_name = method_spec
            method_params = {}
        elif isinstance(method_spec, dict):
            method_name, method_params = next(iter(method_spec.items()))
        else:
            raise MetricSpecificationError(
                f"Method specification must be str or dict, got: {type(method_spec)}",
                method_spec,
            )

        try:
            method = metrics_methods[method_name]
        except KeyError as e:
            raise MetricsMethodNotFoundError(method_name, list(metrics_methods.keys())) from e

        logger.trace(f"Applying method '{method_name}' to grouped data")

        # Use Dask's groupby.apply() which processes groups in parallel
        # The method is applied to each partition of each group
        grouped = data.groupby(split_by, dropna=False, sort=True)

        # Create a serializable applier (Dask requires deterministic hashing)
        applier = _MethodApplier(method, method_params)

        # Apply the method to each group - this stays lazy!
        result_series = grouped.apply(applier, meta=(None, "object"))

        # Compute the results - this is where Dask parallelizes
        computed_result = result_series.compute()

        # Store with method name
        result_key = method_name
        if method_params:
            param_repr = str(sorted(method_params.items()))
            if len(param_repr) > 50:
                param_hash = hashlib.md5(param_repr.encode()).hexdigest()[:8]
                result_key = f"{method_name}_{param_hash}"
            else:
                param_str = "_".join(f"{k}{v}" for k, v in sorted(method_params.items()))
                result_key = f"{method_name}_{param_str}"

        results_by_method[result_key] = computed_result

    # Now convert the computed results to the appropriate MetricResult types
    for method_name, result_data in results_by_method.items():
        # The result_data is now a pandas Series with MultiIndex (or single index)
        # We need to convert this to the appropriate result type

        if isinstance(result_data.index, pd.MultiIndex):
            # Multiple splits - create DataFrame
            if len(split_by) == 1:
                # Single split but MultiIndex? Should not happen, but handle it
                series = pd.Series(result_data.values, index=result_data.index.get_level_values(0))
                series.index.name = split_by[0]
                result_obj = SeriesResult(metric=metric_name, method=method_name, data=series)
            else:
                # Multiple splits - convert to DataFrame
                df = result_data.reset_index()
                df.columns = list(split_by) + ["value"]
                result_obj = DataFrameResult(
                    metric=metric_name, method=method_name, data=df, value_column="value"
                )
        else:
            # Single split - create Series
            series = pd.Series(result_data.values, index=result_data.index)
            series.index.name = split_by[0] if len(split_by) == 1 else None
            result_obj = SeriesResult(metric=metric_name, method=method_name, data=series)

        store.add(result_obj)
