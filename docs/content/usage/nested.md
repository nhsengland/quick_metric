# Nested Dictionary Format

The default output format for Quick Metric, providing direct programmatic access to results.

## Overview

The nested format returns a dictionary structure: `{'metric_name': {'method_name': result}}`

**Best for:** Programming, direct access to results, backward compatibility

## Example

```python
from quick_metric import metric_method, generate_metrics
import pandas as pd
import numpy as np

@metric_method
def count_records(data):
    return len(data)

@metric_method  
def mean_value(data, column='value'):
    return data[column].mean()

@metric_method
def total_value(data, column='value'):
    return data[column].sum()

@metric_method
def category_breakdown(data):
    return data['category'].value_counts()

# Business data
np.random.seed(42)
data = pd.DataFrame({
    'category': np.random.choice(['Premium', 'Standard', 'Basic'], 100),
    'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
    'value': np.random.randint(10, 1000, 100),
    'status': np.random.choice(['active', 'inactive', 'pending'], 100, p=[0.7, 0.2, 0.1])
})

config = {
    'active_premium': {
        'method': ['count_records', 'mean_value', 'total_value'],
        'filter': {'and': {'status': 'active', 'category': 'Premium'}}
    },
    'category_summary': {
        'method': ['category_breakdown'],
        'filter': {'status': 'active'}
    }
}

# Generate metrics (default format)
results = generate_metrics(data, config)
print(results)
```

**Output:**

```python
{
    'active_premium': {
        'count_records': 22,
        'mean_value': 458.54545454545456,
        'total_value': 10088
    },
    'category_summary': {
        'category_breakdown': category
                              Standard    23
                              Premium     22
                              Basic       21
                              Name: count, dtype: int64
    }
}
```

## Usage Patterns

### Direct Access

```python
# Access specific values
premium_count = results['active_premium']['count_records']  # 22
average_value = results['active_premium']['mean_value']     # 458.55
total_revenue = results['active_premium']['total_value']    # 10088

# Work with complex return types
category_counts = results['category_summary']['category_breakdown']
top_category = category_counts.index[0]  # 'Standard'
```

### Iteration and Processing

```python
# Iterate through all metrics
for metric_name, methods in results.items():
    print(f"Metric: {metric_name}")
    for method_name, value in methods.items():
        print(f"  {method_name}: {value}")

# Extract specific method across all metrics
count_results = {}
for metric_name, methods in results.items():
    if 'count_records' in methods:
        count_results[metric_name] = methods['count_records']

# Build summary statistics
summary = {
    metric: {
        'num_methods': len(methods),
        'has_count': 'count_records' in methods,
        'methods': list(methods.keys())
    }
    for metric, methods in results.items()
}
```

### Error Handling

```python
# Safe access with get()
count = results.get('active_premium', {}).get('count_records', 0)

# Check if metric and method exist
if 'active_premium' in results:
    if 'count_records' in results['active_premium']:
        count = results['active_premium']['count_records']
        
# Handle missing metrics gracefully
def safe_get_metric(results, metric_name, method_name, default=None):
    return results.get(metric_name, {}).get(method_name, default)

count = safe_get_metric(results, 'active_premium', 'count_records', 0)
```

### Business Logic Integration

```python
# Calculate derived metrics
premium_metrics = results['active_premium']
if premium_metrics['count_records'] > 0:
    revenue_per_customer = premium_metrics['total_value'] / premium_metrics['count_records']
else:
    revenue_per_customer = 0

# Business rules and thresholds
def evaluate_performance(metrics):
    evaluations = {}
    for metric_name, methods in metrics.items():
        if 'count_records' in methods:
            count = methods['count_records']
            evaluations[metric_name] = {
                'status': 'healthy' if count >= 20 else 'needs_attention',
                'count': count
            }
    return evaluations

performance = evaluate_performance(results)
```

## When to Use

- **Building applications** that need direct access to specific metrics
- **Simple programmatic access** where you know the metric and method names
- **Backward compatibility** with existing code
- **Performance-critical scenarios** where minimal overhead is needed
- **Integration with business logic** that processes results conditionally