"""
Quick Metric: A framework for quickly creating metrics using YAML configs.

This package provides a simple way to apply filters and methods to pandas
DataFrames based on YAML configuration files. It allows users to define
custom metric methods and configure complex data filtering through
declarative YAML configurations.

The main workflow involves:
1. Define custom metric methods using the @metric_method decorator
2. Create YAML configurations specifying filters and methods to apply
3. Apply the configuration to pandas DataFrames to generate metrics

Examples
--------
Basic usage with the new generate_metrics function:

>>> import pandas as pd
>>> from quick_metric import metric_method, generate_metrics
>>>
>>> @metric_method
... def count_records(data):
...     return len(data)
>>>
>>> data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
>>> config = {
...     'category_a_count': {
...         'method': ['count_records'],
...         'filter': {'category': 'A'}
...     }
... }
>>> results = generate_metrics(data, config)

Legacy usage with interpret_metric_instructions:

>>> import pandas as pd
>>> from quick_metric import metric_method, interpret_metric_instructions
>>>
>>> @metric_method
... def count_records(data):
...     return len(data)
>>>
>>> data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
>>> config = {
...     'category_a_count': {
...         'method': ['count_records'],
...         'filter': {'category': 'A'}
...     }
... }
>>> results = interpret_metric_instructions(data, config)

See Also
--------
method_definitions : Core decorator for registering metric methods
filter : Data filtering functionality
apply_methods : Method application logic
core : Main orchestration functions
"""

from quick_metric.apply_methods import apply_method, apply_methods
from quick_metric.core import (
    generate_metrics,
    interpret_metric_instructions,
    read_metric_instructions,
)
from quick_metric.filter import apply_filter
from quick_metric.method_definitions import METRICS_METHODS, metric_method

__version__ = "0.0.1"

__all__ = [
    "generate_metrics",
    "interpret_metric_instructions",
    "read_metric_instructions",
    "apply_method",
    "apply_methods",
    "apply_filter",
    "metric_method",
    "METRICS_METHODS",
]
