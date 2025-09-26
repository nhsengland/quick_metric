"""
This module contains functions to filter DataFrames based on conditions specified in a dictionary.
The dictionary can contain multiple conditions and combine them using logical operators.
"""

from typing import Any, Dict, Union

import pandas as pd


def evaluate_condition(
    data_df: pd.DataFrame, column: str, value: Union[Dict, Any]
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


def recursive_filter(data_df: pd.DataFrame, filters: Dict) -> pd.Series:
    """
    Recursively applies filters to a DataFrame and returns a boolean filter mask.

    Parameters
    ----------
    data_df : pd.DataFrame
        The DataFrame to filter.
    filters : dict
        A dictionary specifying the filters to apply. The dictionary can contain the keys "and",
        "or", and "not" to combine conditions recursively.

    Returns
    -------
    pd.Series
        A boolean filter mask indicating which rows of the DataFrame match the filters.

    Notes
    -----
    The filters dictionary can have the following structure:
    - {"and": {condition1, condition2, ...}}: All conditions must be met.
    - {"or": {condition1, condition2, ...}}: At least one condition must be met.
    - {"not": {condition}}: The condition must not be met.
    - {column: value}: A single condition to apply to a column.

    Each condition is a key-value pair where the key is the column name and the value is the
    condition to apply to that column.
    """
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
    elif "or" in filters:
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
    elif "not" in filters:
        return ~recursive_filter(data_df, filters["not"])
    else:
        return evaluate_condition(
            data_df, list(filters.keys())[0], list(filters.values())[0]
        )


def apply_filter(data_df: pd.DataFrame, filters: Dict) -> pd.DataFrame:
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
    mask = recursive_filter(data_df, filters)
    return data_df.loc[mask]  # type: ignore[return-value]
