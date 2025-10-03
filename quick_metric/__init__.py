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
Basic usage with generate_metrics:

>>> import pandas as pd
>>> from quick_metric import metric_method, generate_metrics
>>>
>>> @metric_method
... def count_records(data):
...     return len(data)
>>>
>>> @metric_method
... def average_col(data, column='value'):
...     return data[column].mean()
>>>
>>> data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
>>>
>>> # Simple method specification
>>> config1 = {
...     'record_count': {
...         'method': 'count_records',
...         'filter': {}
...     }
... }
>>> results1 = generate_metrics(data, config1)
>>>
>>> # Multiple methods
>>> config2 = {
...     'analysis': {
...         'method': ['count_records', 'average_col'],
...         'filter': {'category': 'A'}
...     }
... }
>>> results2 = generate_metrics(data, config2)
>>>
>>> # Method with parameters
>>> config3 = {
...     'custom_avg': {
...         'method': {'average_col': {'column': 'value'}},
...         'filter': {}
...     }
... }
>>> results3 = generate_metrics(data, config3)

Method discovery:

>>> from quick_metric import metric_method
>>>
>>> # Get information about a specific method
>>> method_info = metric_method('count_records')
>>>
>>> # List all available methods
>>> all_methods = metric_method()
>>> print(sorted(all_methods.keys()))

Note
----
This module provides a clean, minimal public API focused on the most common
use cases. Method specifications support multiple flexible formats:
- Single method: "method": "method_name"
- Multiple methods: "method": ["method1", "method2"]
- Method with parameters: "method": {"method_name": {"param": "value"}}

See Also
--------
method_definitions : Core decorator for registering metric methods
core : Main metric generation functionality
"""

from quick_metric.core import generate_metrics
from quick_metric.method_definitions import metric_method

__version__ = "0.0.1"

__all__ = [
    "generate_metrics",
    "metric_method",
]
