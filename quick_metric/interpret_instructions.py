"""
Main orchestration and workflow functionality for Quick Metric.

This module provides the primary entry points for the quick_metric framework,
handling the complete workflow from YAML configuration reading to metric
result generation. It coordinates between the filtering, method application,
and configuration parsing components.

The module serves as the main interface for users, providing high-level
functions that abstract away the complexity of the underlying filtering
and method application processes.

Functions
---------
read_metric_instructions : Load metric configurations from YAML files
interpret_metric_instructions : Execute complete metric workflow on data

Workflow
--------
1. Load YAML configuration specifying metrics, filters, and methods
2. For each metric specification:
   - Apply filters to subset the input DataFrame
   - Execute specified methods on the filtered data
   - Collect results in a structured dictionary
3. Return comprehensive results for all metrics

Examples
--------
Load configuration from YAML file:

>>> from pathlib import Path
>>> from quick_metric.interpret_instructions import read_metric_instructions
>>>
>>> config_path = Path('metrics.yaml')
>>> instructions = read_metric_instructions(config_path)

Execute complete workflow:

>>> import pandas as pd
>>> from quick_metric.interpret_instructions import (
...     interpret_metric_instructions
... )
>>> from quick_metric.method_definitions import metric_method
>>>
>>> @metric_method
... def count_records(data):
...     return len(data)
>>>
>>> data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
>>> config = {
...     'category_metrics': {
...         'method': ['count_records'],
...         'filter': {'category': 'A'}
...     }
... }
>>> results = interpret_metric_instructions(data, config)
>>> print(results['category_metrics']['count_records'])
2

YAML Configuration Format
--------------------------
```yaml
metric_instructions:
  metric_name:
    method: ['method1', 'method2']
    filter:
      column_name: value
      and:
        condition1: value1
        condition2: value2
```

See Also
--------
filter : Data filtering functionality used by this module
apply_methods : Method execution functionality used by this module
method_definitions : Method registration system used by this module
"""

from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import yaml

from quick_metric.apply_methods import apply_methods
from quick_metric.filter import apply_filter
from quick_metric.method_definitions import METRICS_METHODS


def read_metric_instructions(metric_config_path: Path) -> Dict:
    """
    Read metric_instructions dictionary from a YAML config file.

    Parameters
    ----------
    metric_config_path : Path
        Path to the YAML config file containing metric instructions.

    Returns
    -------
    Dict
        The 'metric_instructions' dictionary from the YAML file.
    """
    with open(metric_config_path, "r", encoding="utf-8") as file:
        metric_configs = yaml.safe_load(file)
        metric_instructions = metric_configs.get("metric_instructions", {})
    return metric_instructions


def interpret_metric_instructions(
    data: pd.DataFrame,
    metric_instructions: Dict,
    metrics_methods: Optional[Dict] = None,
) -> Dict:
    """
    Apply filters and methods from metric instructions to a DataFrame.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to be processed.
    metric_instructions : Dict
        Dictionary containing the metrics and their filter/method conditions.
    metrics_methods : Dict, optional
        Dictionary of available methods. Defaults to METRICS_METHODS.

    Returns
    -------
    Dict
        Dictionary with metric names as keys and method results as values.
    """
    if metrics_methods is None:
        metrics_methods = METRICS_METHODS

    results = {}

    for metric_name, metric_instruction in metric_instructions.items():
        # Apply filter to data
        filtered_data = apply_filter(data_df=data, filters=metric_instruction["filter"])

        # Apply methods to filtered data
        results[metric_name] = apply_methods(
            data=filtered_data,
            method_names=metric_instruction["method"],
            metrics_methods=metrics_methods,
        )

    return results
