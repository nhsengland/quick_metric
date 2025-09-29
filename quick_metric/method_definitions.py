"""
This module contains the decorator to register metric methods.

The decorator allows users to register their own custom metric methods
that can be used with the quick_metric framework.
"""

from typing import Callable

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
