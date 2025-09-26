"""
This module contains methods for calculating metrics and the decorator
to register new methods.
"""

from typing import Callable

import pandas as pd

METRICS_METHODS = {}


def metric_method(func: Callable) -> Callable:
    """
    Decorator to register a function as a metric method.
    Adds the function to the METRICS_METHODS dictionary using its name as key.

    Parameters
    ----------
    func : Callable
        The function to be registered as a metric method.

    Returns
    -------
    Callable
        The original function, unmodified.

    Side Effects
    ------------
    Registers the function in the METRICS_METHODS dictionary.
    """
    METRICS_METHODS[func.__name__] = func
    return func


# Example metric methods that can be used as templates
@metric_method
def count_records(data: pd.DataFrame) -> int:
    """Count the number of records in the data."""
    return len(data)


@metric_method
def mean_value(data: pd.DataFrame, column: str = "value") -> float:
    """Calculate the mean of a specified column."""
    if column in data.columns:
        return data[column].mean()
    return 0.0


@metric_method
def sum_values(data: pd.DataFrame, column: str = "value") -> float:
    """Calculate the sum of a specified column."""
    if column in data.columns:
        return data[column].sum()
    return 0.0


@metric_method
def describe_data(data: pd.DataFrame) -> pd.DataFrame:
    """Return descriptive statistics for the data."""
    return data.describe()
