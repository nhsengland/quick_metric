"""Module for applying metric methods to filtered data."""

from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from quick_metric.method_definitions import METRICS_METHODS


class MetricsMethodNotFoundError(ValueError):
    """
    Exception raised when a specified method is not found in metrics methods.
    """


def apply_method(
    data: pd.DataFrame,
    method_name: str,
    metrics_methods: Optional[Dict[str, Callable]] = None,
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

    try:
        method = metrics_methods[method_name]
    except KeyError as e:
        raise MetricsMethodNotFoundError(
            f"Method {method_name} not found in {list(metrics_methods.keys())}"
        ) from e

    return method(data)


def apply_methods(
    data: pd.DataFrame,
    method_names: List[str],
    metrics_methods: Optional[Dict[str, Callable]] = None,
) -> Dict[str, Any]:
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

    results = {}
    for method_name in method_names:
        results[method_name] = apply_method(data, method_name, metrics_methods)

    return results
