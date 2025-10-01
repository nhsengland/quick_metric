# Method Definitions

The method definitions module provides the core decorator and registry system for registering custom metric functions.

## Key Components

### @metric_method Decorator

The `@metric_method` decorator is the primary way to register custom metric functions:

```python
from quick_metric import metric_method

@metric_method
def count_records(data):
    """Count the number of records in the DataFrame."""
    return len(data)

@metric_method
def mean_value(data, column='value'):
    """Calculate mean of a specified column."""
    return data[column].mean() if column in data.columns else 0.0
```

### Thread-Safe Registry

All registered methods are stored in a thread-safe registry that can be accessed concurrently from multiple processes or pipeline stages.

## API Reference

::: quick_metric.method_definitions
    options:
      show_root_heading: false
      show_source: false