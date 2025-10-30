"""
MetricsStore: Registry and query interface for MetricResult objects.

The store provides O(1) direct access and efficient dimension-based filtering
through an inverted index.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from quick_metric.results import (
    DataFrameResult,
    MetricResult,
    ScalarResult,
    SeriesResult,
    create_result,
)


@dataclass
class MetricsStore:
    """
    Container for MetricResult objects with efficient querying.

    The store maintains:
    - Results indexed by (metric, method) for O(1) direct access
    - Dimension index for O(1) dimension filtering
    - Each result\'s dimensions are intrinsic to its data structure

    Examples
    --------
    >>> # Direct access O(1)
    >>> result = store[\'total_cases\', \'count\']
    >>> result.data

    >>> # Filter by dimensions efficiently
    >>> jan_results = store.filter(month=\'2025-01\')
    >>> r0a_results = store.filter(site=\'R0A\')

    >>> # Convenience methods
    >>> counts = store.by_method(\'count_records\')
    >>> premium_metrics = store.by_metric(\'active_premium\')
    """

    _results: dict[tuple[str, str], MetricResult] = field(default_factory=dict)
    _dimension_index: dict[tuple[str, Any], set[tuple[str, str]]] = field(
        default_factory=lambda: defaultdict(set)
    )

    def add(self, result: MetricResult) -> None:
        """
        Add a result to the store and update dimension index.

        Parameters
        ----------
        result : MetricResult
            Result to add
        """
        key = (result.metric, result.method)
        self._results[key] = result

        # Index by dimensions for fast filtering
        # For each dimension value in the result, add to index
        for record in result.to_records():
            for dim_name in result.dimensions():
                if dim_name in record:
                    dim_key = (dim_name, record[dim_name])
                    self._dimension_index[dim_key].add(key)

    def add_from_method(
        self, metric: str, method: str, data: Any, value_column: str | None = None
    ) -> MetricResult:
        """
        Add a result by creating it from method output data.

        This is a convenience method that automatically handles different
        return types from metric methods and adds the result to the store.

        Parameters
        ----------
        metric : str
            Metric name
        method : str
            Method name
        data : scalar, pd.Series, pd.DataFrame, dict, or MetricResult
            Result data from a metric method
        value_column : str, optional
            For DataFrame results, which column contains values

        Returns
        -------
        MetricResult
            The created and added result

        Examples
        --------
        >>> store = MetricsStore()
        >>> store.add_from_method('total', 'count', 100)
        >>> store.add_from_method('monthly', 'count', pd.Series([10, 20]))
        >>> store.add_from_method('detailed', 'analyze', {'data': df, 'value_column': 'count'})
        """
        result = create_result(metric, method, data, value_column)
        self.add(result)
        return result

    def __len__(self) -> int:
        """Number of results in the store."""
        return len(self._results)

    def __getitem__(self, key: tuple[str, str]) -> MetricResult:
        """Direct O(1) access by (metric, method)."""
        return self._results[key]

    def __contains__(self, key: tuple[str, str]) -> bool:
        """Check if (metric, method) exists."""
        return key in self._results

    def get(self, metric: str, method: str, default=None) -> MetricResult | None:
        """Get result with optional default."""
        return self._results.get((metric, method), default)

    def value(self, metric: str, method: str):
        """Get the raw data value (shortcut for .data attribute)."""
        return self._results[(metric, method)].data

    def keys(self) -> Iterator[tuple[str, str]]:
        """Iterate over (metric, method) keys."""
        return iter(self._results.keys())

    def metrics(self) -> list[str]:
        """Get unique metric names."""
        return sorted({m for m, _ in self._results})

    def methods(self, metric: str | None = None) -> list[str]:
        """Get method names, optionally filtered by metric."""
        if metric:
            return sorted([meth for m, meth in self._results if m == metric])
        return sorted({meth for _, meth in self._results})

    def filter(
        self,
        metric: str | list[str] | None = None,
        method: str | list[str] | None = None,
        value_type: str | list[str] | None = None,
        **dimension_filters,
    ) -> MetricsStore:
        """
        Filter results by any combination of attributes.

        Uses dimension index for O(1) dimension lookups on large stores.

        Parameters
        ----------
        metric : str or list[str], optional
            Filter by metric name(s)
        method : str or list[str], optional
            Filter by method name(s)
        value_type : str or list[str], optional
            Filter by value type (\'scalar\', \'series\', \'dataframe\')
        **dimension_filters
            Filter by dimension values (e.g., month=\'2025-01\', site=\'R0A\')

        Returns
        -------
        MetricsStore
            New filtered store

        Examples
        --------
        >>> # Filter by method
        >>> counts = store.filter(method=\'count_records\')

        >>> # Filter by dimension - O(1) lookup!
        >>> jan_data = store.filter(month=\'2025-01\')
        >>> r0a_jan = store.filter(month=\'2025-01\', site=\'R0A\')

        >>> # Combine filters
        >>> jan_scalars = store.filter(month=\'2025-01\', value_type=\'scalar\')
        """
        # Normalize to lists for consistent handling
        metrics = ([metric] if isinstance(metric, str) else metric) if metric is not None else None
        methods = ([method] if isinstance(method, str) else method) if method is not None else None
        value_types = (
            ([value_type] if isinstance(value_type, str) else value_type)
            if value_type is not None
            else None
        )

        # Start with all keys or use dimension index if available
        if dimension_filters:
            # Use dimension index for O(1) filtering
            # Get intersection of all dimension filters
            candidate_keys = None
            for dim_name, dim_value in dimension_filters.items():
                dim_key = (dim_name, dim_value)
                keys_for_this_dim = self._dimension_index.get(dim_key, set())

                if candidate_keys is None:
                    candidate_keys = keys_for_this_dim.copy()
                else:
                    candidate_keys &= keys_for_this_dim

            if candidate_keys is None:
                candidate_keys = set()
        else:
            candidate_keys = set(self._results.keys())

        # Apply remaining filters
        filtered = MetricsStore()
        for key in candidate_keys:
            result = self._results[key]
            result_metric, result_method = key

            # Check metric filter
            if metrics and result_metric not in metrics:
                continue

            # Check method filter
            if methods and result_method not in methods:
                continue

            # Check value_type filter
            if value_types and result.value_type() not in value_types:
                continue

            filtered.add(result)

        return filtered

    def by_method(self, method: str | list[str]) -> MetricsStore:
        """Get all results using a specific method."""
        return self.filter(method=method)

    def by_dimension(self, **dimension_values) -> MetricsStore:
        """Filter by dimension values (uses inverted index for O(1))."""
        return self.filter(**dimension_values)

    def by_metric(self, metric: str | list[str]) -> MetricsStore:
        """Get all results for a specific metric."""
        return self.filter(metric=metric)

    def scalars(self) -> Iterator[tuple[str, str, ScalarResult]]:
        """Iterate over scalar results."""
        for (metric, method), result in self._results.items():
            if isinstance(result, ScalarResult):
                yield metric, method, result

    def series(self) -> Iterator[tuple[str, str, SeriesResult]]:
        """Iterate over series results."""
        for (metric, method), result in self._results.items():
            if isinstance(result, SeriesResult):
                yield metric, method, result

    def dataframes(self) -> Iterator[tuple[str, str, DataFrameResult]]:
        """Iterate over dataframe results."""
        for (metric, method), result in self._results.items():
            if isinstance(result, DataFrameResult):
                yield metric, method, result

    def all(self) -> Iterator[tuple[str, str, MetricResult]]:
        """Iterate over all results."""
        for (metric, method), result in self._results.items():
            yield metric, method, result

    def to_dataframe(self) -> pd.DataFrame:
        """Export all results to a single flat DataFrame (tidy format)."""
        all_records = []
        for result in self._results.values():
            all_records.extend(result.to_records())

        if not all_records:
            return pd.DataFrame()

        return pd.DataFrame(all_records)

    def to_records(self, include_metadata: bool = False) -> list[dict[str, Any]]:
        """
        Convert to flat list of record dictionaries.

        Parameters
        ----------
        include_metadata : bool, default False
            If True, includes value_type and dimensions in each record

        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries, one per data point

        Examples
        --------
        >>> records = store.to_records()
        [
            {'metric': 'completion_rate', 'method': 'overall', 'value': 0.95},
            {'metric': 'completion_rate', 'method': 'by_site', 'site': 'R0A', 'value': 0.92},
            ...
        ]

        >>> records_meta = store.to_records(include_metadata=True)
        [
            {'metric': 'completion_rate', 'method': 'overall', 'value': 0.95,
             'value_type': 'scalar', 'dimensions': [], 'n_dimensions': 0},
            ...
        ]
        """
        all_records = []

        for _, _, result in self.all():
            records = result.to_records()

            if include_metadata:
                # Add metadata to each record
                metadata = {
                    "value_type": result.value_type(),
                    "dimensions": result.dimensions(),
                    "n_dimensions": len(result.dimensions()),
                }
                for record in records:
                    record.update(metadata)

            all_records.extend(records)

        return all_records

    def to_nested_dict(self, include_metadata: bool = False) -> dict[str, dict[str, Any]]:
        """
        Export as nested dictionary: {metric: {method: data}}.

        Parameters
        ----------
        include_metadata : bool, default False
            If True, wraps each result with metadata

        Returns
        -------
        dict[str, dict[str, Any]]
            Nested dictionary grouped by metric

        Examples
        --------
        >>> nested = store.to_nested_dict()
        {
            'completion_rate': {
                'overall': 0.95,
                'by_site': pd.Series([...])
            }
        }

        >>> nested_meta = store.to_nested_dict(include_metadata=True)
        {
            'completion_rate': {
                'overall': {'data': 0.95, 'value_type': 'scalar', ...}
            }
        }
        """
        nested = {}
        for (metric, method), result in self._results.items():
            if metric not in nested:
                nested[metric] = {}

            if include_metadata:
                nested[metric][method] = {
                    "data": result.data,
                    "value_type": result.value_type(),
                    "dimensions": result.dimensions(),
                }
            else:
                nested[metric][method] = result.data

        return nested

    def to_dataframes(self, separate_scalars: bool = True) -> dict[str, pd.DataFrame]:
        """
        Convert to dictionary of DataFrames, one per metric.

        Parameters
        ----------
        separate_scalars : bool, default True
            If True, scalars go to '_scalars' key;
            if False, scalars become single-row DataFrames

        Returns
        -------
        dict[str, pd.DataFrame]
            Dictionary mapping metric names to DataFrames

        Examples
        --------
        >>> dfs = store.to_dataframes()
        {
            '_scalars': DataFrame with all scalar values,
            'completion_rate': DataFrame with all methods for this metric,
            'visit_counts': DataFrame with all methods for this metric
        }
        """
        dataframes = {}
        scalar_records = []

        for metric, _, result in self.all():
            records = result.to_records()

            if result.value_type() == "scalar" and separate_scalars:
                # Collect scalars separately
                scalar_records.extend(records)
            else:
                # Create or append to metric-specific DataFrame
                df = pd.DataFrame(records)

                if metric in dataframes:
                    dataframes[metric] = pd.concat([dataframes[metric], df], ignore_index=True)
                else:
                    dataframes[metric] = df

        # Add scalars DataFrame if any exist
        if scalar_records:
            dataframes["_scalars"] = pd.DataFrame(scalar_records)

        return dataframes

    def to_dict_of_series(self) -> dict[tuple[str, str], pd.Series | Any]:
        """
        Convert to dictionary mapping (metric, method) to Series.

        Returns
        -------
        dict[tuple[str, str], pd.Series | Any]
            Dict with (metric, method) tuples as keys, Series/scalars as values

        Examples
        --------
        >>> series_dict = store.to_dict_of_series()
        {
            ('completion_rate', 'overall'): 0.95,
            ('completion_rate', 'by_site'): pd.Series([0.92, 0.98], index=['R0A', 'R0B'])
        }

        Note
        ----
        Converts single-column DataFrame results to Series automatically
        """
        result = {}

        for metric, method, metric_result in self.all():
            data = metric_result.data

            # Convert single-column DataFrames to Series
            if isinstance(data, pd.DataFrame) and len(data.columns) == 1:
                data = data.iloc[:, 0]

            result[(metric, method)] = data

        return result

    def to_dict_by_metric(self) -> dict[str, dict[str, Any]]:
        """
        Convert to dictionary grouped by metric: {metric: {method: data}}.

        Alias for to_nested_dict() for clarity.

        Returns
        -------
        dict[str, dict[str, Any]]
            Nested dict grouped by metric
        """
        return self.to_nested_dict()

    def to_dict_by_method(self) -> dict[str, dict[str, Any]]:
        """
        Convert to dictionary grouped by method: {method: {metric: data}}.

        Useful when comparing same method across metrics.

        Returns
        -------
        dict[str, dict[str, Any]]
            Nested dict grouped by method

        Examples
        --------
        >>> by_method = store.to_dict_by_method()
        {
            'overall': {
                'completion_rate': 0.95,
                'visit_count': 1234
            },
            'by_site': {
                'completion_rate': pd.Series([...]),
                'visit_count': pd.Series([...])
            }
        }
        """
        result = {}

        for metric, method, metric_result in self.all():
            if method not in result:
                result[method] = {}

            result[method][metric] = metric_result.data

        return result

    def to_datasets(self) -> dict[str, dict[str, pd.DataFrame]]:
        """
        Convert to nested dict of DataFrames: {metric: {method: DataFrame}}.

        Converts all results (including scalars and Series) to DataFrames.

        Returns
        -------
        dict[str, dict[str, pd.DataFrame]]
            Nested dict with all values as DataFrames

        Examples
        --------
        >>> datasets = store.to_datasets()
        {
            'completion_rate': {
                'overall': DataFrame([[0.95]]),
                'by_site': DataFrame with site index
            }
        }
        """
        result = {}

        for metric, method, metric_result in self.all():
            if metric not in result:
                result[metric] = {}

            # Convert to DataFrame
            data = metric_result.data

            if isinstance(data, pd.DataFrame):
                df = data
            elif isinstance(data, pd.Series):
                df = data.to_frame(name="value")
            else:  # scalar
                df = pd.DataFrame({"value": [data]})

            result[metric][method] = df

        return result

    def summary(self) -> pd.DataFrame:
        """Get summary table of what\'s in the store."""
        summaries = []
        for (metric, method), result in self._results.items():
            summaries.append(
                {
                    "metric": metric,
                    "method": method,
                    "value_type": result.value_type(),
                    "dimensions": result.dimensions(),
                    "n_dimensions": len(result.dimensions()),
                }
            )

        if not summaries:
            return pd.DataFrame()

        return pd.DataFrame(summaries)
