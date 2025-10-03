# Usage Guide

Quick Metric provides a simple yet powerful way to create and apply metrics to pandas DataFrames. This guide covers the essential concepts and workflows.

## Overview

Quick Metric provides multiple interfaces for different use cases:

- **`generate_metrics()`** - Main API for generating metrics
- **Pipeline Stage** - For integration with `oops-its-a-pipeline` workflows
- **Output Formats** - Multiple formats for different analytical needs

## Creating Metric Methods

### Basic Metric Methods

All metric functionality starts with the `@metric_method` decorator:

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
def total_value(data, column='value'):
    """Calculate sum of a specified column."""
    return data[column].sum()
```

### Complex Return Types

Quick Metric preserves complex return types like DataFrames and Series:

```python
@metric_method
def category_breakdown(data):
    """Return count breakdown by category."""
    return data['category'].value_counts()

@metric_method
def regional_summary(data):
    """Return detailed breakdown by region."""
    return data.groupby('region').agg({
        'value': ['count', 'mean', 'sum']
    })
```

## Basic Usage

### Complete Example

```python
from quick_metric import metric_method, generate_metrics
import pandas as pd
import numpy as np

# Sample business data
np.random.seed(42)
data = pd.DataFrame({
    'category': np.random.choice(['Premium', 'Standard', 'Basic'], 100),
    'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
    'value': np.random.randint(10, 1000, 100),
    'status': np.random.choice(['active', 'inactive', 'pending'], 100, p=[0.7, 0.2, 0.1])
})

# Business metrics configuration
config = {
    'active_premium': {
        'method': ['count_records', 'mean_value', 'total_value'],
        'filter': {'and': {'status': 'active', 'category': 'Premium'}}
    },
    'regional_analysis': {
        'method': ['count_records', 'mean_value'],
        'filter': {'region': ['North', 'South']}
    },
    'category_summary': {
        'method': ['category_breakdown'],
        'filter': {'status': 'active'}
    }
}

# Generate metrics
results = generate_metrics(data, config)
```

This produces comprehensive business metrics across different segments and regions.

## Choosing Your Output Format

Quick Metric supports four output formats, each optimized for different use cases. The choice of format determines how your metric results are structured and accessed:

### Format Comparison

| Format | Structure | Best For |
|--------|-----------|----------|
| **[Nested Dictionary](nested.md)** | `{'metric': {'method': result}}` | Programming, direct access |
| **[DataFrame](dataframe.md)** | pandas DataFrame | Analysis, visualization, export |
| **[Records](records.md)** | List of dictionaries | APIs, databases, JSON |
| **[Flat DataFrame](flat_dataframe.md)** | Flattened with grouping | Advanced analytics, complex data |

### Quick Preview

Using the same configuration above:

```python
# Nested format (default) - direct access
nested = generate_metrics(data, config)
premium_count = nested['active_premium']['count_records']  # 22

# DataFrame format - analysis ready
df = generate_metrics(data, config, output_format='dataframe')
# Returns structured DataFrame with metric/method/value columns

# Records format - API ready  
records = generate_metrics(data, config, output_format='records')
# Returns list of {'metric': 'name', 'method': 'name', 'value': result}

# Flat DataFrame format - analytics ready
flat = generate_metrics(data, config, output_format='flat_dataframe')
# Returns flattened structure preserving complex groupings
```

Choose the format that matches your workflow - click on any format above for detailed examples and usage patterns.

### Using YAML Configuration

For external configuration management:

```yaml
# config/metrics.yaml
metric_instructions:
  performance_metrics:
    method: ['count_records', 'mean_value']
    filter:
      and:
        status: active
        value: {'>=': 100}
```

```python
from pathlib import Path

config_path = Path('config/metrics.yaml')
results = generate_metrics(data, config_path)
```

## Next Steps

- **[Configuration Guide](../configuration.md)** - Advanced filtering and YAML syntax
- **[Output Formats](nested.md)** - Choose the right format for your use case
- **[Pipeline Integration Guide](../pipeline_integration.md)** - Using Quick Metric in data processing workflows
- **[API Reference](../api_reference/index.md)** - Complete API documentation