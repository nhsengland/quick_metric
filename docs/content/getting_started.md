# Getting Started

This guide will help you get started with **Quick Metric** and begin creating metrics for your data analysis workflows.

## Installation

### Prerequisites

Quick Metric requires Python 3.9+ and works best with pandas DataFrames.

### Using Git Clone (Current)

    git clone <repository-url>
    cd quick_metric
    uv venv && source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    uv pip install -e .

### Future Package Installation

    # Coming soon
    pip install quick-metric
    # or
    uv add quick-metric

## Basic Usage

### 1. Define Custom Metric Methods

The foundation of Quick Metric is the `@metric_method` decorator which registers your custom functions:

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

@metric_method
def percentage_above_threshold(data, column='value', threshold=100):
    """Calculate percentage of records above a threshold."""
    if len(data) == 0:
        return 0.0
    above_threshold = len(data[data[column] > threshold])
    return (above_threshold / len(data)) * 100
```

### 2. Prepare Your Data

Quick Metric works with pandas DataFrames:

```python
import pandas as pd

data = pd.DataFrame({
    'category': ['A', 'B', 'A', 'C', 'B', 'A'],
    'value': [120, 80, 150, 200, 90, 110],
    'status': ['active', 'inactive', 'active', 'active', 'active', 'inactive'],
    'region': ['North', 'South', 'North', 'East', 'South', 'North']
})
```

### 3. Create Configuration

Define which metrics to calculate and what filters to apply:

#### Dictionary Configuration (Recommended)

```python
config = {
    'active_category_a': {
        'method': ['count_records', 'mean_value'],
        'filter': {
            'and': {
                'category': 'A',
                'status': 'active'
            }
        }
    },
    'high_value_analysis': {
        'method': ['count_records', 'percentage_above_threshold'],
        'filter': {
            'value': {'greater than': 100}
        }
    },
    'regional_summary': {
        'method': ['count_records', 'mean_value'],
        'filter': {
            'region': ['North', 'East']  # Multiple values = isin condition
        }
    }
}
```

#### YAML Configuration

```yaml
# metrics.yaml
metric_instructions:
  active_category_a:
    method: ['count_records', 'mean_value']
    filter:
      and:
        category: A
        status: active
  
  high_value_analysis:
    method: ['count_records', 'percentage_above_threshold']
    filter:
      value:
        greater than: 100
```

### 4. Generate Metrics

Use the main entry point to generate your metrics:

```python
from quick_metric import generate_metrics
from pathlib import Path

# Option 1: Dictionary configuration
results = generate_metrics(data, config)

# Option 2: YAML file
results = generate_metrics(data, Path('metrics.yaml'))

# Access results
print(f"Active A records: {results['active_category_a']['count_records']}")
print(f"Mean value: {results['active_category_a']['mean_value']}")
print(f"High value percentage: {results['high_value_analysis']['percentage_above_threshold']}")
```

## Advanced Features

### Complex Filtering

Quick Metric supports sophisticated filtering logic:

```python
complex_config = {
    'complex_analysis': {
        'method': ['count_records', 'mean_value'],
        'filter': {
            'and': {
                'status': 'active',
                'value': {'greater than equal': 100},
                'or': {
                    'category': 'A',
                    'region': ['North', 'East']
                },
                'not': {
                    'category': 'B'
                }
            }
        }
    }
}
```

### Pipeline Integration

For complex workflows, integrate with oops-its-a-pipeline:

```python
from oops_its_a_pipeline import Pipeline, PipelineConfig
from quick_metric.pipeline import create_metrics_stage

class MetricsConfig(PipelineConfig):
    model_config = {'arbitrary_types_allowed': True}
    data: pd.DataFrame = your_data
    config: dict = your_config

# Simple pipeline
pipeline = Pipeline(MetricsConfig()).add_stage(create_metrics_stage())
results = pipeline.run("metrics_analysis")

# Multi-stage pipeline  
pipeline = (Pipeline(config)
    .add_function_stage(load_data, outputs="data")
    .add_function_stage(prepare_config, outputs="config")
    .add_stage(create_metrics_stage())
    .add_function_stage(save_results, inputs="metrics"))
```

### Error Handling

Quick Metric provides comprehensive error handling:

```python
try:
    results = generate_metrics(data, config)
except ValueError as e:
    print(f"Configuration error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Key Concepts

### Method Registration

- Use `@metric_method` decorator to register functions
- Functions must accept a DataFrame as the first parameter
- Additional parameters are supported with defaults
- Methods are automatically available across your application

### Filtering Logic

- **Simple equality**: `column: value`
- **List membership**: `column: [value1, value2]` (isin condition)
- **Logical operators**: `and`, `or`, `not`
- **Comparisons**: `greater than`, `less than`, `greater than equal`, `less than equal`
- **Set operations**: `in`, `not in`, `is`

### Result Structure

Results are returned as nested dictionaries:

```python
{
    'metric_name': {
        'method1_name': result1,
        'method2_name': result2
    }
}
```

## Next Steps

- Explore the [API Reference](api_reference/index.md) for detailed documentation
- Review specific modules:
    - [Core Functions](api_reference/core.md) - Main entry points
    - [Method Definitions](api_reference/method_definitions.md) - Decorator and registry
    - [Filter](api_reference/filter.md) - Data filtering logic
    - [Apply Methods](api_reference/apply_methods.md) - Method execution
    - [Pipeline](api_reference/pipeline.md) - oops-its-a-pipeline integration
- Check out practical examples in the repository tests
