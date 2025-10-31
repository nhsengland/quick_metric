"""
Method application and execution functionality for Quick Metric.

Handles the application of registered metric methods to filtered pandas
DataFrames with error handling for missing methods.

Functions
---------
apply_method : Apply a single metric method to data
apply_methods : Apply multiple metric methods to data and collect results

Examples
--------
Apply a single method:

```python
import pandas as pd
```python
from quick_metric.registry import metric_method
from quick_metric._apply_methods import apply_method

@metric_method
def count_rows(data):
    return len(data)

data = pd.DataFrame({'a': [1, 2, 3]})
result = apply_method('count_rows', data)
print(result)  # 3
```

Apply multiple methods:

```python
from quick_metric._apply_methods import apply_methods

@metric_method
def sum_column(data, column='a'):
    return data[column].sum()

methods = ['count_rows', 'sum_column']
results = apply_methods(methods, data)
print(results)  # {'count_rows': 3, 'sum_column': 6}
```
"""

import hashlib
from typing import Any, Callable, Optional

from loguru import logger
import pandas as pd

from quick_metric.exceptions import (
    MetricsMethodNotFoundError,
    MetricSpecificationError,
)
from quick_metric.registry import METRICS_METHODS, MetricMethod
from quick_metric.store import MetricsStore


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
    store: Optional[MetricsStore] = None,
    metric_name: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """
    Apply multiple methods to the data.
    The methods are specified by their names or as parameter dictionaries and are
    looked up in the metrics_methods dictionary.
    The results are returned in a dictionary where the keys are method names
    (potentially with parameters) and the values are the results of applying the methods.

    If store and metric_name are provided, results are added directly to the store
    instead of being collected in a dict (more efficient, avoids secondary loop).

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
    store : MetricsStore, optional
        If provided, results will be added directly to this store instead of
        being returned in a dictionary. Requires metric_name to also be provided.
    metric_name : str, optional
        The metric name to use when adding results to the store. Required if
        store is provided.

    Returns
    -------
    Dict[str, Any] or None
        If store is None: A dictionary where the keys are the method names
        (with parameters if any) and the values are the results of applying the methods.
        If store is provided: None (results added directly to store)
    """
    if not metrics_methods:
        metrics_methods = METRICS_METHODS

    if store is not None and metric_name is None:
        raise ValueError("metric_name must be provided when store is provided")

    logger.debug(f"Applying {len(method_specs)} methods: {method_specs}")

    # If store provided, add results directly (no intermediate dict)
    if store is not None:
        assert metric_name is not None  # Checked above
        for method_spec in method_specs:
            # Parse method spec
            if isinstance(method_spec, str):
                method_name_str = method_spec
                method_params = {}
                result_key = method_name_str
            elif isinstance(method_spec, dict):
                method_name_str, method_params = next(iter(method_spec.items()))
                # Generate result key with params
                if method_params:
                    param_repr = str(sorted(method_params.items()))
                    if len(param_repr) > 50:
                        param_hash = hashlib.md5(param_repr.encode()).hexdigest()[:8]
                        result_key = f"{method_name_str}_{param_hash}"
                    else:
                        param_str = "_".join(f"{k}{v}" for k, v in sorted(method_params.items()))
                        result_key = f"{method_name_str}_{param_str}"
                else:
                    result_key = method_name_str
            else:
                raise MetricSpecificationError(
                    f"Method specification must be str or dict, got: {type(method_spec)}",
                    method_spec,
                )

            # Get the method
            try:
                method = metrics_methods[method_name_str]
            except KeyError as e:
                logger.error(f"Method '{method_name_str}' not found in available methods")
                raise MetricsMethodNotFoundError(
                    method_name_str, list(metrics_methods.keys())
                ) from e

            # If it's a MetricMethod, use apply() to get MetricResult directly
            if isinstance(method, MetricMethod):
                result = method.apply(metric_name, data, **method_params)
                # add() expects MetricResult and doesn't need conversion
                store.add(result)
            else:
                # Plain callable - use old path
                result_value = method(data, **method_params) if method_params else method(data)
                store.add_from_method(metric_name, result_key, result_value)

        logger.success(f"Successfully applied all {len(method_specs)} methods")
        return None

    # Legacy behavior: collect results in dict
    results = {}
    for method_spec in method_specs:
        result_key, result_value = apply_method(data, method_spec, metrics_methods)
        results[result_key] = result_value

    logger.success(f"Successfully applied all {len(method_specs)} methods")
    return results
