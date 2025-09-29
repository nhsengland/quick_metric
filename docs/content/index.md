# Quick Metric

Welcome to **Quick Metric** - a powerful NHS England Data Science framework designed for creating and applying custom analytical methods. This library provides a flexible, standardized approach to defining, filtering, and applying analytical methods across healthcare datasets.

!!! info "Purpose"

    Quick Metric empowers data scientists and analysts to:
    
    - Define custom analytical methods with standardized interfaces
    - Apply filtering logic to determine when methods should be used
    - Execute methods on datasets with consistent error handling
    - Interpret and validate method instructions and parameters

## Key Features

* **[Method Definitions](api_reference/method_definitions.md)** - Create standardized analytical method definitions
* **[Apply Methods](api_reference/apply_methods.md)** - Execute methods on datasets with robust error handling  
* **[Filter Logic](api_reference/filter.md)** - Intelligent filtering to determine method applicability
* **[Instruction Interpretation](api_reference/interpret_instructions.md)** - Parse and validate method instructions and parameters

## Quick Start

Install Quick Metric using your preferred package manager:

```bash
# Using pip
pip install quick-metric

# Using uv
uv add quick-metric
```

Basic usage example:

```python
from quick_metric import apply_methods, method_definitions

# Define your analytical method
def my_analysis_method(data, **kwargs):
    """Custom analysis method."""
    return data.mean()

# Register the method
method_def = method_definitions.create_method(
    name="mean_analysis",
    function=my_analysis_method,
    description="Calculate mean values"
)

# Apply to your data
result = apply_methods.execute_method(method_def, your_data)
```

## Getting Started

New to Quick Metric? Start with our [Getting Started](getting_started.md) guide to learn the fundamentals and begin using the framework in your analytical workflows.

## API Reference

Explore the complete [API Reference](api_reference/index.md) for detailed documentation of all modules, classes, and functions.
