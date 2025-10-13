"""
Quick Metric framework for creating metrics from pandas DataFrames.

A framework for applying filters and methods to pandas DataFrames using
YAML configurations. Register custom metric methods with decorators and
configure data filtering through declarative specifications.

Functions
---------
generate_metrics : Apply metric configurations to pandas DataFrames
metric_method : Decorator to register custom metric functions
get_method : Retrieve a registered metric method by name
list_method_names : List all registered metric method names
get_registered_methods : Get dictionary of all registered methods
clear_methods : Clear all registered methods from registry

Examples
--------
Basic usage:

```python
import pandas as pd
from quick_metric import metric_method, generate_metrics

@metric_method
def count_records(data):
    return len(data)

@metric_method
def mean_value(data, column='value'):
    return data[column].mean()

data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [10, 20, 30]})
config = {
    'category_a_metrics': {
        'method': ['count_records', 'mean_value'],
        'filter': {'category': 'A'}
    }
}

results = generate_metrics(data, config)
print(results['category_a_metrics']['count_records'])  # 2
print(results['category_a_metrics']['mean_value'])     # 20.0
```
"""

from quick_metric._core import generate_metrics
from quick_metric._method_definitions import (
    clear_methods,
    get_method,
    get_registered_methods,
    list_method_names,
    metric_method,
)

__version__ = "1.0.0"

__all__ = [
    "generate_metrics",
    "metric_method",
    "get_method",
    "get_registered_methods",
    "list_method_names",
    "clear_methods",
]
