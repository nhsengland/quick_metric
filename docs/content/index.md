# Quick Metric

Welcome to **Quick Metric** - a powerful framework for quickly creating metrics using easy-to-edit YAML configs and reusable methods to filter, calculate, and transform data.

!!! info "Purpose"

    Quick Metric empowers data scientists and analysts to:
    
    - Define custom metric methods using simple decorators
    - Configure complex data filtering via YAML or dictionary configurations
    - Apply multiple metrics to pandas DataFrames consistently
    - Integrate metrics generation into data processing pipelines
    - Keep metric definitions separate from data processing logic

## Key Features

* **[@metric_method Decorator](api_reference/method_definitions.md)** - Register custom metric functions with a simple decorator
* **[Core Functions](api_reference/core.md)** - Main entry points: `generate_metrics()` and `interpret_metric_instructions()`
* **[Data Filtering](api_reference/filter.md)** - Complex filtering logic with YAML configuration support
* **[Method Application](api_reference/apply_methods.md)** - Execute methods on filtered data with error handling
* **[Pipeline Integration](api_reference/pipeline.md)** - Seamless integration with oops-its-a-pipeline workflows

## Quick Start

Install Quick Metric and its dependencies:

```bash
# Clone and install
git clone <repository-url>
cd quick_metric
uv venv && source .venv/bin/activate
uv pip install -e .
```

Basic usage example:

```python
from quick_metric import metric_method, generate_metrics
import pandas as pd

# Define custom metric methods
@metric_method
def count_records(data):
    """Count the number of records."""
    return len(data)

@metric_method  
def mean_value(data, column='value'):
    """Calculate mean of a column."""
    return data[column].mean() if column in data.columns else 0.0

# Create data and configuration
data = pd.DataFrame({
    'category': ['A', 'B', 'A', 'C'],
    'value': [10, 20, 15, 30],
    'status': ['active', 'inactive', 'active', 'active']
})

config = {
    'active_category_a': {
        'method': ['count_records', 'mean_value'],
        'filter': {
            'and': {
                'category': 'A',
                'status': 'active'
            }
        }
    }
}

# Generate metrics
results = generate_metrics(data, config)
print(results['active_category_a']['count_records'])  # 2
print(results['active_category_a']['mean_value'])     # 12.5
```

## Pipeline Integration

Quick Metric integrates seamlessly with oops-its-a-pipeline:

```python
from oops_its_a_pipeline import Pipeline, PipelineConfig
from quick_metric.pipeline import create_metrics_stage

class Config(PipelineConfig):
    model_config = {'arbitrary_types_allowed': True}
    data = your_dataframe
    config = your_metrics_config

pipeline = Pipeline(Config()).add_stage(create_metrics_stage())
results = pipeline.run("analysis")
```

## Getting Started

New to Quick Metric? Start with our [Getting Started](getting_started.md) guide to learn the fundamentals and begin using the framework in your analytical workflows.

## API Reference

Explore the complete [API Reference](api_reference/index.md) for detailed documentation of all modules, classes, and functions.
