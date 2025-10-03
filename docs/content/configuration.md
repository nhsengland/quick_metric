# Configuration Guide

This guide covers advanced configuration techniques, filtering syntax, and method parameters for Quick Metric.

## Configuration Formats

Quick Metric uses YAML configuration files by default, with dictionary configuration available for programmatic use.

=== "YAML Configuration (Default)"

    YAML is the primary configuration format for Quick Metric, providing a clean and readable way to define metrics:

    ```yaml
    metric_instructions:
      cancer_metrics:
        method: ['count_records', 'mean_value']
        filter:
          and:
            disease_type: Cancer
            status: Active
            not:
              remove: Remove
      
      rare_disease_metrics:
        method: ['count_records']
        filter:
          and:
            disease_type: 'Rare Disease'
            status: Active
    ```

    YAML configurations are loaded using:

    ```python
    from quick_metric import generate_metrics

    # Load from YAML file
    results = generate_metrics(data, "config/metrics.yaml")
    ```

=== "Dictionary Configuration"

    For programmatic use and dynamic configuration, dictionaries provide maximum flexibility:

    ```python
    config = {
        'metric_name': {
            'method': ['method1', 'method2'],
            'filter': {
                'column': 'value',
                'numeric_column': {'>=': 100},
                'and': {
                    'status': 'active',
                    'not': {'remove': 'Remove'}
                }
            }
        }
    }

    # Use directly with generate_metrics
    results = generate_metrics(data, config)
    ```

## Advanced Filtering

### Logical Operators

```python
# AND conditions
'filter': {
    'and': {
        'status': 'active',
        'category': 'A',
        'value': {'>=': 100}
    }
}

# OR conditions  
'filter': {
    'or': {
        'priority': 'high',
        'value': {'>=': 1000}
    }
}

# NOT conditions
'filter': {
    'not': {
        'status': 'deleted'
    }
}

# Complex nested logic
'filter': {
    'and': {
        'status': 'active',
        'or': {
            'category': 'premium',
            'value': {'>=': 500}
        },
        'not': {
            'flag': 'exclude'
        }
    }
}
```

### Comparison Operators

```python
'filter': {
    'numeric_value': {'>=': 100},      # Greater than or equal
    'other_value': {'<=': 50},         # Less than or equal
    'exact_value': {'==': 42},         # Exact match
    'not_value': {'!=': 0},            # Not equal
    'range_value': {'>=': 10, '<=': 100}  # Range (AND condition)
}
```

### Working with Lists and Multiple Values

```python
# Multiple acceptable values (OR condition)
'filter': {
    'category': ['A', 'B', 'C']  # category in ['A', 'B', 'C']
}

# Exclude multiple values
'filter': {
    'not': {
        'status': ['deleted', 'archived', 'hidden']
    }
}
```

## Method Parameters

### Parameterized Methods

Quick Metric supports method parameters through the configuration. You can specify parameters using a dictionary format:

```python
@metric_method
def percentile_value(data, column='value', percentile=50):
    """Calculate percentile of a column."""
    return data[column].quantile(percentile / 100)

@metric_method  
def top_n_sum(data, column='value', n=10):
    """Sum of top N values."""
    return data.nlargest(n, column)[column].sum()

# Configuration with parameters
config = {
    'quartile_analysis': {
        'method': [
            {'percentile_value': {'percentile': 25}},
            {'percentile_value': {'percentile': 75}},
            {'top_n_sum': {'n': 5}}
        ],
        'filter': {'status': 'active'}
    }
}
```

### Mixed Configurations

You can mix parameterized and non-parameterized methods in the same configuration:

```python
config = {
    'analysis': {
        'method': [
            'percentile_value',  # Uses default percentile=50
            {'percentile_value': {'percentile': 90}},  # Custom percentile
            {'top_n_sum': {'n': 3}}  # Custom n value
        ],
        'filter': {'status': 'active'}
    }
}
```

### Result Keys

When using parameters, the result keys include the parameter values for uniqueness:

- `percentile_value` (default parameters) → `percentile_value`
- `{'percentile_value': {'percentile': 25}}` → `percentile_value_percentile25`
- `{'top_n_sum': {'n': 5}}` → `top_n_sum_n5`

### Default Parameters

Methods with default parameters will use those defaults when called without explicit parameters:

```python
@metric_method
def value_above_threshold(data, column='value', threshold=100):
    """Count records above threshold."""
    return len(data[data[column] > threshold])

# Configuration using default and custom parameters
config = {
    'threshold_analysis': {
        'method': [
            'value_above_threshold',  # Uses threshold=100 (default)
            {'value_above_threshold': {'threshold': 50}},  # Custom threshold
            {'value_above_threshold': {'threshold': 200}}  # Another custom threshold
        ],
        'filter': {}
    }
}
```

### Parameter Validation

The system validates that:

- Method dictionaries contain exactly one method
- Parameters are provided as dictionaries
- Method names exist in the registry
- Parameters match the method signature

```python
# ✅ Valid configurations
{'method_name': {'param': value}}
'method_name'

# ❌ Invalid configurations  
{'method1': {}, 'method2': {}}  # Multiple methods in one dict
{'method_name': 'not_a_dict'}   # Parameters not a dict
```

## Tips and Best Practices

### Performance Optimization

1. **Order filters by selectivity** - Put most selective filters first
2. **Use appropriate data types** - Ensure numeric comparisons use numeric types
3. **Minimize method complexity** - Break complex calculations into smaller methods

### Configuration Management

1. **Use descriptive metric names** - Makes results easier to interpret
2. **Group related metrics** - Organize configurations by business domain
3. **Document complex filters** - Add comments in YAML for clarity
4. **Version control configurations** - Track changes to metric definitions
