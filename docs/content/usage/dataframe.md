# DataFrame Format

Ideal for data analysis and export operations.

## Overview

Returns a pandas DataFrame with columns: `metric`, `method`, `value`, `value_type`

**Best for:** Analysis, exporting to files, integration with pandas workflows

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
    'regional_analysis': {
        'method': ['count_records', 'mean_value'],
        'filter': {'region': ['North', 'South']}
    }
}

# Generate metrics as DataFrame
df_results = generate_metrics(data, config, output_format="dataframe")
print(df_results)
```

**Output:**

```
              metric         method        value value_type
0     active_premium  count_records    22.000000        int
1     active_premium     mean_value   458.545455    float64
2     active_premium    total_value 10088.000000      int64
3  regional_analysis  count_records    41.000000        int
4  regional_analysis     mean_value   548.731707    float64
```

## Usage Patterns

### Filtering and Analysis

```python
# Filter by method type
counts = df_results[df_results['method'] == 'count_records']

# Group by metric for summary
metric_summary = df_results.groupby('metric')['value'].agg(['count', 'mean', 'sum'])

# Filter by value type
numeric_metrics = df_results[df_results['value_type'].isin(['int', 'float64'])]

# Filter by value thresholds
high_values = df_results[df_results['value'] > 100]
```

### Export Operations

```python
# Export to CSV
df_results.to_csv('business_metrics.csv', index=False)

# Export to Excel with formatting
with pd.ExcelWriter('metrics_report.xlsx') as writer:
    df_results.to_excel(writer, sheet_name='Metrics', index=False)
    
    # Add summary sheet
    summary = df_results.groupby('metric')['value'].agg(['count', 'mean'])
    summary.to_excel(writer, sheet_name='Summary')

# Export to JSON
df_results.to_json('metrics_output.json', orient='records', indent=2)
```

### Data Manipulation

```python
# Pivot for comparison
pivot_df = df_results.pivot(index='metric', columns='method', values='value')
print(pivot_df)

# Add calculated columns
df_results['value_category'] = pd.cut(df_results['value'], 
                                     bins=[0, 50, 500, float('inf')], 
                                     labels=['Low', 'Medium', 'High'])

# Merge with metadata
metric_metadata = pd.DataFrame({
    'metric': ['active_premium', 'regional_analysis'],
    'business_unit': ['Sales', 'Operations'],
    'priority': ['High', 'Medium']
})
enriched = df_results.merge(metric_metadata, on='metric', how='left')
```

## When to Use

- **Data analysis workflows** where you need to manipulate results
- **Exporting results** to CSV, Excel, or other formats  
- **Integration with pandas-based workflows**
- **Statistical analysis** across multiple metrics
- **Reporting pipelines** that process structured data