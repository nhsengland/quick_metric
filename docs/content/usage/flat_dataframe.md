# Flat DataFrame Format

Optimized for advanced analytics with tuple-based grouping.

## Overview

Returns a flattened DataFrame with columns: `metric`, `method`, `group_by`, `statistic`, `metric_value`

**Best for:** Advanced analytics, complex grouping, statistical analysis

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
        'method': ['count_records', 'mean_value'],
        'filter': {'and': {'status': 'active', 'category': 'Premium'}}
    },
    'category_summary': {
        'method': ['category_breakdown'],
        'filter': {'status': 'active'}
    }
}

# Generate metrics as flat DataFrame
flat_df = generate_metrics(data, config, output_format="flat_dataframe")
print(flat_df)
```

**Output:**

```text
             metric            method  group_by statistic  metric_value
0    active_premium     count_records      None     value     22.000000
1    active_premium        mean_value      None     value    458.545455
2  category_summary  category_breakdown  Standard     count     23.000000
3  category_summary  category_breakdown   Premium     count     22.000000
4  category_summary  category_breakdown     Basic     count     21.000000
```

## Column Structure

- **`metric`**: Metric name from configuration
- **`method`**: Method name that was applied  
- **`group_by`**: Grouping variables (tuples for multi-level, None for ungrouped)
- **`statistic`**: Measured statistic (tuples for MultiIndex columns, strings for simple)
- **`metric_value`**: Computed value

## Usage Patterns

### Filtering Operations

```python
# Filter by statistic type
value_metrics = flat_df[flat_df['statistic'] == 'value']
count_metrics = flat_df[flat_df['statistic'] == 'count']

# Handle grouped vs ungrouped data
ungrouped = flat_df[flat_df['group_by'].isna()]
grouped = flat_df[flat_df['group_by'].notna()]

# Filter by specific groups
premium_data = flat_df[flat_df['group_by'] == 'Premium']
standard_data = flat_df[flat_df['group_by'] == 'Standard']

# Complex group filtering for tuple-based groupings
q1_north = flat_df[flat_df['group_by'].apply(
    lambda x: x and isinstance(x, tuple) and x == ('Q1', 'North')
)]
```

### Advanced Analytics

```python
# Pivot analysis
pivot_table = flat_df.pivot_table(
    index='group_by', 
    columns='metric', 
    values='metric_value', 
    aggfunc='first'
)

# Statistical analysis across groups
group_stats = flat_df.groupby(['group_by', 'statistic'])['metric_value'].agg([
    'count', 'mean', 'std', 'min', 'max'
])

# Compare metrics across different groups
comparison = flat_df.pivot_table(
    index='group_by',
    columns=['metric', 'method'],
    values='metric_value'
)
```

### Working with Complex Groupings

```python
# Extract grouping components for multi-level groupings
def extract_group_level(group_by, level=0):
    if isinstance(group_by, tuple) and len(group_by) > level:
        return group_by[level]
    return None

flat_df['quarter'] = flat_df['group_by'].apply(lambda x: extract_group_level(x, 0))
flat_df['region'] = flat_df['group_by'].apply(lambda x: extract_group_level(x, 1))

# Handle different statistic types
def extract_stat_name(statistic):
    if isinstance(statistic, tuple):
        return statistic[0]  # First element of tuple
    return statistic

flat_df['stat_name'] = flat_df['statistic'].apply(extract_stat_name)

# Aggregate across grouping levels
quarterly_summary = flat_df.groupby([
    flat_df['group_by'].apply(lambda x: x[0] if isinstance(x, tuple) else x),
    'metric'
])['metric_value'].sum()
```

### Business Intelligence Operations

```python
# Create KPI dashboard data
kpi_data = flat_df.groupby('metric').agg({
    'metric_value': ['sum', 'mean', 'count'],
    'group_by': 'nunique'
}).round(2)

# Performance benchmarking
benchmarks = flat_df.groupby('group_by')['metric_value'].agg([
    ('p25', lambda x: x.quantile(0.25)),
    ('median', 'median'),
    ('p75', lambda x: x.quantile(0.75))
])
```

## Grouping Behavior

- **Scalar results**: `group_by = None`
- **Series with meaningful index**: `group_by = index_values`
- **DataFrames with default index**: `group_by = None`
- **DataFrames with meaningful grouping**: `group_by = group_values`
- **Multi-level grouping**: `group_by = (level1, level2, ...)`

## When to Use

- **Complex grouped data analysis** where you need to preserve grouping structure
- **Statistical analysis** across multiple dimensions
- **Advanced analytics** that require flattened data structure
- **Integration with data science workflows** that expect long-form data
- **When working with MultiIndex DataFrames** from metric methods
- **Business intelligence dashboards** that aggregate across multiple dimensions
