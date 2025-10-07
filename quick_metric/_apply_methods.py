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

    Examples:
        >>> import pandas as pd
        >>> from quick_metric._method_definitions import metric_method
        >>> from quick_metric._apply_methods import apply_method
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

    >>> from quick_metric._apply_methods import apply_methods
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
_method_definitions : Module for registering methods
_filter : Module for data filtering before method application
"""

import hashlib
from typing import Any, Callable, Optional

from loguru import logger
import pandas as pd

from quick_metric._exceptions import (
    MetricsMethodNotFoundError,
    MetricSpecificationError,
)
from quick_metric._method_definitions import METRICS_METHODS


def apply_method(
    data: pd.DataFrame,
    method_spec: str | dict,
    metrics_methods: Optional[dict[str, Callable]] = None,
) -> tuple[str, Any]:
    """
    Apply the specified method to the filtered data from metrics methods.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the filtered data.
    method_spec : str or dict
        The method specification. Can be either:
        - str: The name of the method to be applied
        - dict: A dictionary with method name as key and parameters as value
               e.g., {'method_name': {'param1': value1, 'param2': value2}}
    metrics_methods : Dict[str, Callable], optional
        A dictionary of metrics methods. Defaults to METRICS_METHODS.
        This dictionary maps method names to their corresponding functions.

    Returns
    -------
    tuple[str, Any]
        A tuple containing (result_key, result) where result_key is the key
        to use in the results dictionary and result is the method output.

    Raises
    -------
    MetricsMethodNotFoundError
        If the specified method is not found in the metrics methods.
    """
    if not metrics_methods:
        metrics_methods = METRICS_METHODS

    # Parse method specification
    if isinstance(method_spec, str):
        method_name = method_spec
        method_params = {}
        result_key = method_name
    elif isinstance(method_spec, dict):
        if len(method_spec) != 1:
            raise MetricSpecificationError(
                f"Method specification must contain exactly one method, got: {method_spec}",
                method_spec,
            )
        method_name, method_params = next(iter(method_spec.items()))
        if not isinstance(method_params, dict):
            raise MetricSpecificationError(
                f"Method parameters must be a dictionary, got: {type(method_params)}", method_spec
            )
        # Create a result key that includes parameters for uniqueness
        if method_params:
            # For complex parameters, use a hash to keep names manageable
            param_repr = str(sorted(method_params.items()))
            if len(param_repr) > 50:  # If parameter representation is too long
                param_hash = hashlib.md5(param_repr.encode()).hexdigest()[:8]
                result_key = f"{method_name}_{param_hash}"
            else:
                # For simple parameters, use readable format
                param_str = "_".join(f"{k}{v}" for k, v in sorted(method_params.items()))
                result_key = f"{method_name}_{param_str}"
        else:
            result_key = method_name
    else:
        raise MetricSpecificationError(
            f"Method specification must be str or dict, got: {type(method_spec)}", method_spec
        )

    logger.trace(f"Applying method '{method_name}' with params {method_params} to {len(data)} rows")

    try:
        method = metrics_methods[method_name]
    except KeyError as e:
        logger.error(f"Method '{method_name}' not found in available methods")
        raise MetricsMethodNotFoundError(method_name, list(metrics_methods.keys())) from e

    try:
        # Call method with parameters
        result = method(data, **method_params) if method_params else method(data)
        logger.success(f"Method '{method_name}' completed successfully")
        return result_key, result
    except Exception as e:
        logger.critical(f"Error applying method '{method_name}': {e}")
        raise


def apply_methods(
    data: pd.DataFrame,
    method_specs: list[str | dict],
    metrics_methods: Optional[dict[str, Callable]] = None,
) -> dict[str, Any]:
    """
    Apply multiple methods to the data.
    The methods are specified by their names or as parameter dictionaries and are
    looked up in the metrics_methods dictionary.
    The results are returned in a dictionary where the keys are method names
    (potentially with parameters) and the values are the results of applying the methods.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame containing the filtered data.
    method_specs : List[str | dict]
        A list of method specifications. Each can be either:
        - str: The name of the method to be applied
        - dict: A dictionary with method name as key and parameters as value
    metrics_methods : Dict[str, Callable], optional
        A dictionary of metrics methods. Defaults to METRICS_METHODS.
        This dictionary maps method names to their corresponding functions.

    Returns
    -------
    Dict[str, Any]
        A dictionary where the keys are the method names (with parameters if any)
        and the values are the results of applying the methods.
    """
    if not metrics_methods:
        metrics_methods = METRICS_METHODS

    logger.debug(f"Applying {len(method_specs)} methods: {method_specs}")

    results = {}
    for method_spec in method_specs:
        result_key, result_value = apply_method(data, method_spec, metrics_methods)
        results[result_key] = result_value

    logger.success(f"Successfully applied all {len(method_specs)} methods")
    return results
