# Records Format

Perfect for APIs, databases, and JSON serialization.

## Overview

Returns a list of dictionaries, where each record contains: `metric`, `method`, `value`, `value_type`

**Best for:** APIs, databases, JSON serialization

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

# Generate metrics as records
records = generate_metrics(data, config, output_format="records")
print(records)
```

**Output:**

```python
[
    {'metric': 'active_premium', 'method': 'count_records', 'value': 22, 'value_type': 'int'},
    {'metric': 'active_premium', 'method': 'mean_value', 'value': 458.54545454545456, 'value_type': 'float64'},
    {'metric': 'active_premium', 'method': 'total_value', 'value': 10088, 'value_type': 'int64'},
    {'metric': 'regional_analysis', 'method': 'count_records', 'value': 41, 'value_type': 'int'},
    {'metric': 'regional_analysis', 'method': 'mean_value', 'value': 548.7317073170732, 'value_type': 'float64'}
]
```

## Usage Patterns

### JSON Serialization

```python
import json

# Convert to JSON with proper formatting
json_output = json.dumps(records, indent=2, default=str)
print(json_output)

# Save to file
with open('business_metrics.json', 'w') as f:
    json.dump(records, f, indent=2, default=str)

# Load and process JSON
with open('business_metrics.json', 'r') as f:
    loaded_records = json.load(f)
```

### Database Operations

```python
import pandas as pd
from sqlalchemy import create_engine

# Bulk insert into database
df = pd.DataFrame(records)
engine = create_engine('sqlite:///business_metrics.db')
df.to_sql('metrics_log', engine, if_exists='append', index=False)

# Custom database insertion with metadata
for record in records:
    # Add timestamp and session info
    enhanced_record = {
        **record,
        'timestamp': datetime.now().isoformat(),
        'session_id': current_session_id,
        'environment': 'production'
    }
    insert_metric_record(enhanced_record)

# Batch processing for large datasets
batch_size = 1000
for i in range(0, len(records), batch_size):
    batch = records[i:i + batch_size]
    process_batch(batch)
```

### API Integration

```python
from flask import Flask, jsonify
from fastapi import FastAPI

# Flask API endpoint
app = Flask(__name__)

@app.route('/api/metrics')
def get_metrics():
    records = generate_metrics(data, config, output_format="records")
    return jsonify({
        'status': 'success',
        'data': records,
        'count': len(records)
    })

# FastAPI endpoint with response model
from pydantic import BaseModel
from typing import List

class MetricRecord(BaseModel):
    metric: str
    method: str
    value: float
    value_type: str

@app.get("/metrics", response_model=List[MetricRecord])
async def get_metrics():
    return generate_metrics(data, config, output_format="records")
```

### Data Processing and Filtering

```python
# Filter by method type
count_records = [r for r in records if r['method'] == 'count_records']
aggregates = [r for r in records if r['method'] in ['mean_value', 'total_value']]

# Filter by value thresholds
high_value_metrics = [r for r in records if r['value'] > 100]

# Group by metric
from collections import defaultdict
grouped = defaultdict(list)
for record in records:
    grouped[record['metric']].append(record)

# Transform for specific use cases
api_response = [
    {
        'id': f"{r['metric']}_{r['method']}",
        'name': f"{r['metric'].replace('_', ' ').title()} - {r['method'].replace('_', ' ').title()}",
        'result': r['value'],
        'data_type': r['value_type']
    }
    for r in records
]

# Calculate summary statistics
summary_stats = {
    'total_metrics': len(records),
    'unique_metrics': len(set(r['metric'] for r in records)),
    'unique_methods': len(set(r['method'] for r in records)),
    'value_ranges': {
        'min': min(r['value'] for r in records if isinstance(r['value'], (int, float))),
        'max': max(r['value'] for r in records if isinstance(r['value'], (int, float)))
    }
}
```

## When to Use

- **Building APIs** that return JSON responses
- **Database storage** where each metric is a separate record
- **Streaming data** where records are processed individually
- **Integration with systems** that expect list-of-dict format
- **Logging and monitoring** where each metric needs individual tracking
- **ETL pipelines** that process metrics as discrete events