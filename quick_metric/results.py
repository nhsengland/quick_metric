"""
MetricResult: Typed containers for different metric output types.

Each result type (Scalar, Series, DataFrame) handles its own data representation
and understands its dimensional structure:
- Scalar: No dimensions (single value)
- Series: One dimension (the index)
- DataFrame: N dimensions (all columns except the value column)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from loguru import logger
import pandas as pd

from quick_metric.exceptions import MetricSpecificationError


@dataclass
class MetricResult(ABC):
    """
    Base class for all metric results.

    Every result has:
    - metric: Name of the metric
    - method: Name of the method that produced it
    - data: The actual result data (contains dimensions implicitly)
    """

    metric: str
    method: str
    data: Any

    @abstractmethod
    def value_type(self) -> str:
        """Return the type identifier ('scalar', 'series', 'dataframe')."""
        ...

    @abstractmethod
    def dimensions(self) -> list[str]:
        """Return list of dimension names in this result."""
        ...

    @abstractmethod
    def to_records(self) -> list[dict[str, Any]]:
        """
        Convert to flat record format (tidy data).

        Each record contains: metric, method, dimension columns, and value.
        - Scalar: One record with just metric, method, value
        - Series: One record per index value
        - DataFrame: One record per row
        """
        ...

    def matches(self, **filters) -> bool:
        """Check if this result matches the given filters."""
        for key, value in filters.items():
            # Check special keys
            if key == "metric" and self.metric != value:
                return False
            if key == "method" and self.method != value:
                return False
            if key == "value_type" and self.value_type() != value:
                return False

            # For dimension filters, check if any record matches
            # This is expensive but correct - we need to look at actual data
            if key not in ("metric", "method", "value_type"):
                records = self.to_records()
                if not any(record.get(key) == value for record in records):
                    return False

        return True


@dataclass
class ScalarResult(MetricResult):
    """
    Single scalar value result (no dimensions).

    Examples
    --------
    >>> result = ScalarResult(
    ...     metric='total_cases',
    ...     method='count',
    ...     data=1500
    ... )
    >>> result.data  # 1500
    >>> result.dimensions()  # []
    """

    data: int | float | str | bool | None = None

    def value_type(self) -> str:
        return "scalar"

    def dimensions(self) -> list[str]:
        """Scalars have no dimensions."""
        return []

    def to_records(self) -> list[dict[str, Any]]:
        """Single record with the scalar value."""
        return [
            {
                "metric": self.metric,
                "method": self.method,
                "value": self.data,
            }
        ]


@dataclass
class SeriesResult(MetricResult):
    """
    pandas Series result (one dimension: the index).

    The Series index represents the single dimension, and values are the metric values.

    Examples
    --------
    >>> result = SeriesResult(
    ...     metric='monthly_count',
    ...     method='count_by_month',
    ...     data=pd.Series([10, 20, 30], index=['2025-01', '2025-02', '2025-03'], name='count')
    ... )
    >>> result.dimensions()  # ['index'] or index name if named
    """

    data: pd.Series = None

    def value_type(self) -> str:
        return "series"

    def dimensions(self) -> list[str]:
        """Series has one dimension: the index."""
        index_name = self.data.index.name if self.data.index.name else "index"
        return [index_name]

    def to_records(self) -> list[dict[str, Any]]:
        """One record per Series element."""
        index_name = self.data.index.name if self.data.index.name else "index"
        value_name = self.data.name if self.data.name else "value"

        records = []
        for idx, val in self.data.items():
            records.append(
                {
                    "metric": self.metric,
                    "method": self.method,
                    index_name: idx,
                    value_name: val,
                }
            )
        return records


@dataclass
class DataFrameResult(MetricResult):
    """
    pandas DataFrame result (multiple dimensions).

    DataFrame must be in tidy format with dimension columns and one value column.
    All columns except the value column are considered dimensions.

    Parameters
    ----------
    metric : str
        Metric name
    method : str
        Method name
    data : pd.DataFrame
        Tidy format DataFrame
    value_column : str, optional
        Name of the value column. If not specified, assumes last column is the value.

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'month': ['2025-01', '2025-01', '2025-02'],
    ...     'standard': ['<7days', '<14days', '<7days'],
    ...     'count': [50, 75, 60]
    ... })
    >>> result = DataFrameResult(
    ...     metric='turnaround',
    ...     method='by_month_standard',
    ...     data=df,
    ...     value_column='count'
    ... )
    >>> result.dimensions()  # ['month', 'standard']
    """

    data: pd.DataFrame = None
    value_column: str | None = None

    def __post_init__(self):
        """Determine value column if not specified."""
        if self.value_column is None and not self.data.empty:
            # Default: last column is the value
            self.value_column = self.data.columns[-1]

    def value_type(self) -> str:
        return "dataframe"

    def dimensions(self) -> list[str]:
        """All columns except the value column are dimensions."""
        if self.data.empty:
            return []
        return [col for col in self.data.columns if col != self.value_column]

    def to_records(self) -> list[dict[str, Any]]:
        """One record per DataFrame row."""
        records = []
        for _, row in self.data.iterrows():
            record = {
                "metric": self.metric,
                "method": self.method,
            }
            # Add all DataFrame columns (dimensions + value)
            record.update(row.to_dict())
            records.append(record)
        return records


def create_result(
    metric: str,
    method: str,
    data: Any,
    value_column: str | None = None,
) -> MetricResult:
    """
    Create appropriate MetricResult based on data type.

    Intelligently handles different return types from metric methods:
    - Direct data: Creates result based on type (scalar/Series/DataFrame)
    - Dict with 'data' key: Extracts data and optional metadata
    - Dict with 'value' key: Treats as scalar result
    - MetricResult subclass: Returns as-is

    Provides helpful logged warnings when making assumptions.

    Parameters
    ----------
    metric : str
        Metric name
    method : str
        Method name
    data : scalar, pd.Series, pd.DataFrame, dict, or MetricResult
        Result data. Can be:
        - Direct value (scalar, Series, DataFrame)
        - Dict with 'data' key and optional 'value_column' key
        - Dict with 'value' key (treated as scalar)
        - Already a MetricResult instance
    value_column : str, optional
        For DataFrame results, which column contains the values.
        Can also be specified in dict under 'value_column' key.

    Returns
    -------
    MetricResult
        ScalarResult, SeriesResult, or DataFrameResult

    Raises
    ------
    ValueError
        If dict format is invalid or data type cannot be handled

    Examples
    --------
    Direct data:
    >>> create_result('total', 'count', 100)  # ScalarResult
    >>> create_result('monthly', 'count', pd.Series([10, 20]))  # SeriesResult
    >>> create_result('detailed', 'analyze', df, value_column='count')  # DataFrameResult

    Dict with metadata:
    >>> create_result('detailed', 'analyze', {
    ...     'data': df,
    ...     'value_column': 'count'
    ... })

    Already a result:
    >>> result = ScalarResult('test', 'method', 42)
    >>> create_result('test', 'method', result)  # Returns same instance
    """
    # Handle if it's already a MetricResult
    if isinstance(data, MetricResult):
        logger.debug(f"Method '{method}' returned MetricResult directly")
        return data

    # Handle dict returns
    if isinstance(data, dict):
        # Check for 'data' key (preferred format)
        if "data" in data:
            actual_data = data["data"]
            # Allow value_column to be specified in dict
            dict_value_column = data.get("value_column", None)
            final_value_column = dict_value_column or value_column

            if dict_value_column and value_column and dict_value_column != value_column:
                logger.warning(
                    f"Method '{method}' for metric '{metric}': "
                    f"value_column specified in both dict ('{dict_value_column}') "
                    f"and parameter ('{value_column}'). Using dict value."
                )

            # Recursively create result from extracted data
            return create_result(metric, method, actual_data, final_value_column)

        # Check for 'value' key (treat as scalar)
        if "value" in data:
            logger.debug(
                f"Method '{method}' for metric '{metric}': "
                f"dict with 'value' key treated as scalar result"
            )
            return ScalarResult(metric=metric, method=method, data=data["value"])

        # Dict without recognized keys - raise error
        raise MetricSpecificationError(
            f"Method '{method}' for metric '{metric}': "
            f"returned dict without 'data' or 'value' keys (got keys: {list(data.keys())}). "
            f"Please return either:"
            f"\n  - Direct data (scalar, Series, DataFrame)"
            f"\n  - {{'data': your_data, 'value_column': 'col_name'}} for DataFrames"
            f"\n  - {{'value': your_scalar}} for scalar values",
            method_spec=data,
        )

    # Handle pandas types
    if isinstance(data, pd.DataFrame):
        if value_column is None:
            logger.debug(
                f"Method '{method}' for metric '{metric}': "
                f"DataFrame returned without value_column specified. "
                f"Using last column '{data.columns[-1]}' as value column."
            )
        return DataFrameResult(
            metric=metric,
            method=method,
            data=data,
            value_column=value_column,
        )
    if isinstance(data, pd.Series):
        return SeriesResult(metric=metric, method=method, data=data)
    # Scalar (int, float, str, bool, None, etc.)
    return ScalarResult(metric=metric, method=method, data=data)
