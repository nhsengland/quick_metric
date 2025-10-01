# Core Functions

The core module provides the main entry points for the Quick Metric framework, orchestrating the complete workflow from configuration to results.

## Functions

### generate_metrics()

::: quick_metric.core.generate_metrics
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

The primary entry point for generating metrics. This function provides a simple interface for processing pandas DataFrames using either YAML configuration files or dictionary configurations.

**Examples:**

Using dictionary configuration:
```python
import pandas as pd
from quick_metric import generate_metrics, metric_method

@metric_method
def count_records(data):
    return len(data)

data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
config = {
    'category_a_count': {
        'method': ['count_records'],
        'filter': {'category': 'A'}
    }
}

results = generate_metrics(data, config)
# Returns: {'category_a_count': {'count_records': 2}}
```

Using YAML configuration:
```python
from pathlib import Path
results = generate_metrics(data, Path('metrics.yaml'))
```

### interpret_metric_instructions()

::: quick_metric.core.interpret_metric_instructions
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

Legacy entry point that processes metric instructions on DataFrame. This function is maintained for backward compatibility.

**Examples:**

```python
from quick_metric import interpret_metric_instructions

instructions = {
    'test_metric': {
        'method': ['count_records'],
        'filter': {'status': 'active'}
    }
}

results = interpret_metric_instructions(data, instructions)
```

### read_metric_instructions()

::: quick_metric.core.read_metric_instructions
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

Utility function to load metric configurations from YAML files.

**Examples:**

```python
from pathlib import Path
from quick_metric import read_metric_instructions

config_path = Path('config/metrics.yaml')
instructions = read_metric_instructions(config_path)
```

## Workflow

The core module coordinates the following workflow:

1. **Configuration Processing**: Parse YAML files or validate dictionary configurations
2. **Instruction Interpretation**: Extract metric definitions, filters, and method lists
3. **Data Processing**: For each metric:
   - Apply filters to subset the DataFrame
   - Execute specified methods on filtered data
   - Collect results in structured format
4. **Result Assembly**: Return comprehensive results dictionary

## Error Handling

The core functions provide comprehensive error handling:

- **FileNotFoundError**: When YAML configuration files don't exist
- **KeyError**: When YAML files don't contain required 'metric_instructions' key
- **ValueError**: When configuration types are invalid
- **Custom Exceptions**: Various method and filtering related errors

All errors include detailed context about the operation that failed and suggested solutions.

## Thread Safety

All core functions are thread-safe and can be used in concurrent environments. The underlying method registry and execution engines handle concurrent access appropriately.