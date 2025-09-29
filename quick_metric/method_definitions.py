"""
Method registration and decorator functionality for Quick Metric.

This module provides the core decorator for registering custom metric methods
that can be used with the quick_metric framework. Methods decorated with
@metric_method are automatically registered in the global METRICS_METHODS
registry and become available for use in YAML configurations.

The module maintains a global registry of all registered metric methods,
allowing the framework to dynamically discover and execute user-defined
metric functions.

Attributes
----------
METRICS_METHODS : dict
    Global registry mapping method names to their corresponding functions.
    Automatically populated when functions are decorated with @metric_method.

Examples
--------
Register a custom metric method:

>>> from quick_metric.method_definitions import metric_method
>>> import pandas as pd
>>>
>>> @metric_method
... def calculate_average(data, column='value'):
...     '''Calculate the average of a specified column.'''
...     return data[column].mean() if column in data.columns else 0.0
>>>
>>> # Method is now available in METRICS_METHODS
>>> print('calculate_average' in METRICS_METHODS)
True

See Also
--------
apply_methods : Module that uses the registered methods
core : Main workflow that orchestrates method execution
"""

from typing import Callable

from loguru import logger

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
    logger.trace(f"Registering metric method: {func.__name__}")
    METRICS_METHODS[func.__name__] = func
    logger.success(f"Metric method '{func.__name__}' registered successfully")
    return func
