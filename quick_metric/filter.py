"""
Data filtering functionality for Quick Metric.

This module provides comprehensive data filtering capabilities for pandas
DataFrames using dictionary-based filter specifications. It supports
complex logical operations including AND, OR, NOT conditions, as well
as various comparison operators and membership tests.

The filtering system is designed to work with nested filter conditions
specified in YAML configurations, allowing for sophisticated data
subset selection before applying metric methods.

Functions
---------
evaluate_condition : Evaluate a single filter condition
recursive_filter : Handle complex nested filter conditions
apply_filter : Main entry point for applying filters to DataFrames

Supported Operators
-------------------
- Equality: `{'column': 'value'}`
- Membership: `{'column': ['value1', 'value2']}`
- Comparisons: `{'column': {'greater than': 10}}`
- Logical: `{'and': {...}}`, `{'or': {...}}`, `{'not': {...}}`
- Set operations: `{'column': {'in': [...]}}`

Examples
--------
Simple equality filter:

>>> import pandas as pd
>>> from quick_metric.filter import apply_filter
>>>
>>> data = pd.DataFrame({
...     'category': ['A', 'B', 'A', 'C'],
...     'value': [10, 20, 30, 40]
... })
>>> filter_spec = {'category': 'A'}
>>> filtered_data = apply_filter(data, filter_spec)
>>> print(len(filtered_data))
2

Complex nested filter:

>>> filter_spec = {
...     'and': {
...         'category': ['A', 'B'],
...         'value': {'greater than': 15}
...     }
... }
>>> filtered_data = apply_filter(data, filter_spec)
>>> print(len(filtered_data))
2

Negation filter:

>>> filter_spec = {
...     'not': {'category': 'C'}
... }
>>> filtered_data = apply_filter(data, filter_spec)
>>> print(len(filtered_data))
3

See Also
--------
interpret_instructions : Module that uses filtering before applying methods
apply_methods : Module that processes filtered data
"""

from typing import Any, Union

from loguru import logger
import pandas as pd


def evaluate_condition(  # noqa: PLR0911
    data_df: pd.DataFrame, column: str, value: Union[dict, Any]
) -> pd.Series:
    """
    Evaluate a condition based on the provided column and value.

    Parameters
    ----------
    data_df : pd.DataFrame
        The DataFrame to be filtered.
    column : str
        The column name to be filtered.
    value : Union[dict, Any]
        The value to be compared against the column.

    Returns
    -------
    pd.Series
        Boolean filter mask indicating whether the condition is met
        for each row in the DataFrame.

    Notes
    -----
    This function supports the following comparison operators:
    - less than
    - less than equal
    - greater than
    - greater than equal
    - is
    - not
    - in
    - not in
    """
    if isinstance(value, dict):
        if "not" in value:
            return ~evaluate_condition(data_df, column, value["not"])
        if "less than" in value:
            return data_df[column] < value["less than"]
        if "less than equal" in value:
            return data_df[column] <= value["less than equal"]
        if "greater than" in value:
            return data_df[column] > value["greater than"]
        if "greater than equal" in value:
            return data_df[column] >= value["greater than equal"]
        if "is" in value:
            return data_df[column] == value["is"]
        if "in" in value:
            return data_df[column].isin(value["in"])
        if "not in" in value:
            return ~data_df[column].isin(value["not in"])
    if column in data_df.columns:
        if isinstance(value, list):
            return data_df[column].isin(value)
        return data_df[column] == value
    # Return a boolean mask with all False values
    return pd.Series(index=data_df.index, data=False, dtype=bool)


def recursive_filter(data_df: pd.DataFrame, filters: dict) -> pd.Series:
    """
    Recursively applies filters to a DataFrame and returns a boolean mask.

    Parameters
    ----------
    data_df : pd.DataFrame
        The DataFrame to filter.
    filters : dict
        A dictionary specifying the filters to apply. The dictionary can
        contain the keys "and", "or", and "not" to combine conditions
        recursively.

    Returns
    -------
    pd.Series
        A boolean filter mask indicating which rows of the DataFrame match
        the filters.

    Notes
    -----
    The filters dictionary can have the following structure:
    - {"and": {condition1, condition2, ...}}: All conditions must be met.
    - {"or": {condition1, condition2, ...}}: At least one condition must
      be met.
    - {"not": {condition}}: The condition must not be met.
    - {column: value}: A single condition to apply to a column.

    Each condition is a key-value pair where the key is the column name
    and the value is the condition to apply to that column.
    """
    # Handle empty filters - return all rows as True
    if not filters:
        return pd.Series(index=data_df.index, data=True, dtype=bool)

    if "and" in filters:
        mask = pd.Series(index=data_df.index, data=True, dtype=bool)
        for key, value in filters["and"].items():
            if key in ["and", "or", "not"]:
                mask &= recursive_filter(data_df, {key: value})
            else:
                condition_result = evaluate_condition(data_df, key, value)
                if condition_result is not None:
                    mask &= condition_result
        return mask
    if "or" in filters:
        mask = pd.Series(index=data_df.index, data=False, dtype=bool)
        or_conditions = filters["or"]

        # Handle both list and dict format for or conditions
        if isinstance(or_conditions, list):
            for condition in or_conditions:
                condition_result = recursive_filter(data_df, condition)
                mask |= condition_result
        else:
            # Dictionary format (original logic)
            for key, value in or_conditions.items():
                if key in ["and", "or", "not"]:
                    condition_result = recursive_filter(data_df, {key: value})
                    mask |= condition_result
                else:
                    condition_result = evaluate_condition(data_df, key, value)
                    if condition_result is not None:
                        mask |= condition_result
        return mask
    if "not" in filters:
        return ~recursive_filter(data_df, filters["not"])
    return evaluate_condition(
        data_df, list(filters.keys())[0], list(filters.values())[0]
    )


def apply_filter(data_df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """
    Apply filters to the DataFrame based on the provided dictionary.

    Parameters
    ----------
    data_df : pd.DataFrame
        The DataFrame to be filtered.
    filters : dict
        Dictionary containing the filter conditions.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame.
    """
    logger.trace(f"Applying filters to DataFrame with {len(data_df)} rows")

    if not filters:
        logger.trace("No filters specified, returning original DataFrame")
        return data_df

    mask = recursive_filter(data_df, filters)
    filtered_df = data_df.loc[mask]  # type: ignore[return-value]

    logger.trace(f"Filter applied: {len(filtered_df)} rows remaining")
    return filtered_df
