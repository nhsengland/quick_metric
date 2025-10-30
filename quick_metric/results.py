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

from quick_metric.exceptions import (
    InvalidResultFormatError,
    ValueColumnConflictWarning,
)


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

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert to DataFrame regardless of underlying type.

        Returns
        -------
        pd.DataFrame
            DataFrame representation with metric and method columns
        """
        records = self.to_records()
        return pd.DataFrame(records)

    def to_dict(self, include_metadata: bool = False) -> dict | Any:
        """
        Convert to dictionary format.

        Parameters
        ----------
        include_metadata : bool, default False
            If True, wraps data with metadata (metric, method, value_type, dimensions)

        Returns
        -------
        dict or Any
            If include_metadata=True, returns dict with metadata.
            If include_metadata=False, returns the raw data.

        Examples
        --------
        >>> result = ScalarResult('total', 'count', 100)
        >>> result.to_dict()  # 100
        >>> result.to_dict(include_metadata=True)
        # {'metric': 'total', 'method': 'count', 'value_type': 'scalar',
        #  'dimensions': [], 'data': 100}
        """
        if include_metadata:
            return {
                "metric": self.metric,
                "method": self.method,
                "value_type": self.value_type(),
                "dimensions": self.dimensions(),
                "data": self.data,
            }

        return self.data

    def to_series(self) -> pd.Series:
        """
        Convert to Series if possible.

        Returns
        -------
        pd.Series
            Series representation of the result

        Raises
        ------
        ValueError
            If result cannot be converted to Series (e.g., multi-column DataFrame)

        Examples
        --------
        >>> result = ScalarResult('total', 'count', 100)
        >>> result.to_series()
        # Series([100], name='total_count')

        >>> result = SeriesResult('by_site', 'count', pd.Series([10, 20], index=['A', 'B']))
        >>> result.to_series()
        # Returns the underlying Series
        """
        if isinstance(self.data, pd.Series):
            return self.data

        if isinstance(self.data, pd.DataFrame):
            if len(self.data.columns) == 1:
                return self.data.iloc[:, 0]

            raise ValueError(
                f"Cannot convert DataFrame with {len(self.data.columns)} "
                "columns to Series. Use .to_dataframe() instead."
            )

        # Scalar
        return pd.Series([self.data], name=f"{self.metric}_{self.method}")

    def __repr__(self) -> str:
        """String representation of the result."""
        return (
            f"{self.__class__.__name__}("
            f"metric='{self.metric}', "
            f"method='{self.method}', "
            f"value_type='{self.value_type()}', "
            f"dimensions={self.dimensions()})"
        )

    def __str__(self) -> str:
        """User-friendly string representation."""
        return f"{self.metric}.{self.method}: {self.value_type()}"

    def _repr_html_(self) -> str:
        """HTML representation for Jupyter notebooks."""
        # Convert to DataFrame for nice rendering
        df = self.to_dataframe()

        html = f"<div><strong>{self.metric}.{self.method}</strong> "
        html += f"<em>({self.value_type()})</em></div>"
        html += df._repr_html_()

        return html

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

    def get_column(self, column: str) -> pd.Series:
        """
        Extract a specific column as Series.

        Parameters
        ----------
        column : str
            Name of column to extract

        Returns
        -------
        pd.Series
            Series for the specified column

        Raises
        ------
        KeyError
            If column does not exist in the DataFrame

        Examples
        --------
        >>> result = DataFrameResult('analysis', 'detailed', df, value_column='count')
        >>> counts = result.get_column('count')
        >>> sites = result.get_column('site')
        """
        if column not in self.data.columns:
            raise KeyError(
                f"Column '{column}' not found. Available columns: {list(self.data.columns)}"
            )
        return self.data[column]


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
                ValueColumnConflictWarning(  # pylint: disable=W0133
                    metric=metric,
                    method=method,
                    dict_value=dict_value_column,
                    param_value=value_column,
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
        raise InvalidResultFormatError(
            metric=metric,
            method=method,
            returned_keys=list(data.keys()),
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
