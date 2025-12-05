"""
Data filtering functionality for Quick Metric.

Provides data filtering capabilities for pandas DataFrames using
dictionary-based filter specifications with logical operations.

Functions
---------
evaluate_condition : Evaluate a single filter condition
recursive_filter : Handle complex nested filter conditions
apply_filter : Main entry point for applying filters to DataFrames

Examples
--------
Simple equality filter:

```python
import pandas as pd
from quick_metric._filter import apply_filter

data = pd.DataFrame({
    'category': ['A', 'B', 'A', 'C'],
    'value': [10, 20, 30, 40]
})
filter_spec = {'category': 'A'}
filtered_data = apply_filter(data, filter_spec)
print(len(filtered_data))  # 2
```

Complex nested filter:

```python
filter_spec = {
    'and': {
        'category': ['A', 'B'],
        'value': {'greater than': 15}
    }
}
filtered_data = apply_filter(data, filter_spec)
print(len(filtered_data))  # 2
```

Negation filter:

```python
filter_spec = {
    'not': {'category': 'C'}
}
filtered_data = apply_filter(data, filter_spec)
print(len(filtered_data))  # 3
```

See Also
--------
interpret_instructions : Module that uses filtering before applying methods
apply_methods : Module that processes filtered data
"""

from typing import Any, Union

import dask.dataframe as dd
from loguru import logger


def evaluate_condition(data_df: dd.DataFrame, column: str, value: Union[dict, Any]) -> dd.Series:
    """
    Evaluate a condition based on the provided column and value.

    Parameters
    ----------
    data_df : dd.DataFrame
        The DataFrame to be filtered.
    column : str
        The column name to be filtered.
    value : Union[dict, Any]
        The value to be compared against the column.

    Returns
    -------
    dd.Series
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
        eval_dict = {
            "not": lambda _, y: ~evaluate_condition(data_df, column, y),
            "less than": lambda x, y: x < y,
            "less than equal": lambda x, y: x <= y,
            "greater than": lambda x, y: x > y,
            "greater than equal": lambda x, y: x >= y,
            "is": lambda x, y: x == y,
            "in": lambda x, y: x.isin(y),
            "not in": lambda x, y: ~x.isin(y),
        }
        # Check if any key in the value dict matches an operator
        for operator, operator_value in value.items():
            if operator in eval_dict:
                return eval_dict[operator](data_df[column], operator_value)
    if column in data_df.columns:
        if isinstance(value, list):
            return data_df[column].isin(value)
        return data_df[column] == value
    # Return a boolean mask with all False values
    return dd.from_pandas(
        dd.utils.make_meta({data_df.index.name or "index": "int64"}),
        npartitions=data_df.npartitions,
    ).map_partitions(lambda x: x.assign(mask=False)["mask"], meta=("mask", "bool"))


def recursive_filter(data_df: dd.DataFrame, filters: dict) -> dd.Series:
    """
    Recursively applies filters to a DataFrame and returns a boolean mask.

    Parameters
    ----------
    data_df : dd.DataFrame
        The DataFrame to filter.
    filters : dict
        A dictionary specifying the filters to apply. The dictionary can
        contain the keys "and", "or", and "not" to combine conditions
        recursively.

    Returns
    -------
    dd.Series
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
        return data_df.index.to_series().map_partitions(lambda x: x.notna(), meta=("index", "bool"))

    if "and" in filters:
        mask = data_df.index.to_series().map_partitions(lambda x: x.notna(), meta=("mask", "bool"))
        for key, value in filters["and"].items():
            if key in ["and", "or", "not"]:
                mask &= recursive_filter(data_df, {key: value})
            else:
                condition_result = evaluate_condition(data_df, key, value)
                if condition_result is not None:
                    mask &= condition_result
        return mask
    if "or" in filters:
        mask = data_df.index.to_series().map_partitions(lambda x: x * False, meta=("mask", "bool"))
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
    return evaluate_condition(data_df, list(filters.keys())[0], list(filters.values())[0])


def apply_filter(data_df: dd.DataFrame, filters: dict) -> dd.DataFrame:
    """
    Apply filters to the DataFrame based on the provided dictionary.

    Parameters
    ----------
    data_df : dd.DataFrame
        The DataFrame to be filtered.
    filters : dict
        Dictionary containing the filter conditions.

    Returns
    -------
    dd.DataFrame
        Filtered DataFrame.
    """
    logger.trace(f"Applying filters to DataFrame with {data_df.npartitions} partitions")

    if not filters:
        logger.trace("No filters specified, returning original DataFrame")
        return data_df

    mask = recursive_filter(data_df, filters)
    filtered_df = data_df[mask]

    logger.trace(f"Filter applied: {filtered_df.npartitions} partitions remaining")
    return filtered_df
