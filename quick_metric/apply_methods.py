"""
Method application and execution functionality for Quick Metric.

This module handles the application of registered metric methods to filtered
pandas DataFrames. It provides both single method application and batch
method application capabilities, with comprehensive error handling for
missing methods.

The module acts as the execution engine for the quick_metric framework,
taking registered methods from the METRICS_METHODS registry and applying
them to data with proper error handling and result collection.

Functions
---------
apply_method : Apply a single metric method to data
apply_methods : Apply multiple metric methods to data and collect results

Exceptions
----------
MetricsMethodNotFoundError : Raised when a requested method is not registered

Examples
--------
Apply a single method:

>>> import pandas as pd
>>> from quick_metric.method_definitions import metric_method
>>> from quick_metric.apply_methods import apply_method
>>>
>>> @metric_method
... def count_rows(data):
...     return len(data)
>>>
>>> data = pd.DataFrame({'a': [1, 2, 3]})
>>> result = apply_method('count_rows', data)
>>> print(result)
3

Apply multiple methods:

>>> from quick_metric.apply_methods import apply_methods
>>>
>>> @metric_method
... def sum_column(data, column='a'):
...     return data[column].sum()
>>>
>>> methods = ['count_rows', 'sum_column']
>>> results = apply_methods(methods, data)
>>> print(results)
{'count_rows': 3, 'sum_column': 6}

See Also
--------
method_definitions : Module for registering methods
filter : Module for data filtering before method application
"""

from typing import Any, Callable, Optional

from loguru import logger
import pandas as pd

from quick_metric.method_definitions import METRICS_METHODS


class MetricsMethodNotFoundError(ValueError):
    """
    Exception raised when a specified method is not found in metrics methods.
    """


def apply_method(
    data: pd.DataFrame,
    method_name: str,
    metrics_methods: Optional[dict[str, Callable]] = None,
) -> Any:
    """
    Apply the specified method to the filtered data from metrics methods.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the filtered data.
    method_name : str
        The name of the method to be applied.
    metrics_methods : Dict[str, Callable], optional
        A dictionary of metrics methods. Defaults to METRICS_METHODS.
        This dictionary maps method names to their corresponding functions.

    Returns
    -------
    Any
        The result of applying the method to the filtered data.

    Raises
    -------
    MetricsMethodNotFoundError
        If the specified method is not found in the metrics methods.
    """
    if not metrics_methods:
        metrics_methods = METRICS_METHODS

    logger.trace(f"Applying method '{method_name}' to {len(data)} rows")

    try:
        method = metrics_methods[method_name]
    except KeyError as e:
        logger.error(f"Method '{method_name}' not found in available methods")
        raise MetricsMethodNotFoundError(
            f"Method {method_name} not found in {list(metrics_methods.keys())}"
        ) from e

    try:
        result = method(data)
        logger.success(f"Method '{method_name}' completed successfully")
        return result
    except Exception as e:
        logger.critical(f"Error applying method '{method_name}': {e}")
        raise


def apply_methods(
    data: pd.DataFrame,
    method_names: list[str],
    metrics_methods: Optional[dict[str, Callable]] = None,
) -> dict[str, Any]:
    """
    Apply multiple methods to the data.
    The methods are specified by their names and are looked up in the
    metrics_methods dictionary.
    The results are returned in a dictionary where the keys are method names
    and the values are the results of applying the methods.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the filtered data.
    method_names : List[str]
        A list of method names to be applied.
    metrics_methods : Dict[str, Callable], optional
        A dictionary of metrics methods. Defaults to METRICS_METHODS.
        This dictionary maps method names to their corresponding functions.

    Returns
    -------
    Dict[str, Any]
        A dictionary where the keys are the method names and the values are
        the results of applying the methods.
    """
    if not metrics_methods:
        metrics_methods = METRICS_METHODS

    logger.debug(f"Applying {len(method_names)} methods: {method_names}")

    results = {}
    for method_name in method_names:
        results[method_name] = apply_method(data, method_name, metrics_methods)

    logger.success(f"Successfully applied all {len(method_names)} methods")
    return results
