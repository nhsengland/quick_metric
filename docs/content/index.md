# Quick Metric

[![RAP Status: Gold](https://img.shields.io/badge/RAP_Status-Gold-gold)](https://nhsdigital.github.io/rap-community-of-practice/introduction_to_RAP/levels_of_RAP/#gold-rap---analysis-as-a-product, "Gold RAP")
[![Python: 3.10 | 3.11 | 3.12](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/downloads/ "Python 3.10, 3.11, 3.12")
[![Code Style: Ruff](https://img.shields.io/badge/Code%20Style-Ruff-D7FF64.svg)](https://github.com/astral-sh/ruff)
[![Linting: Ruff](https://img.shields.io/badge/Linting-Ruff-red.svg)](https://github.com/astral-sh/ruff)
[![Testing: tox](https://img.shields.io/badge/Testing-tox-green.svg)](https://tox.readthedocs.io/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![licence: MIT](https://img.shields.io/badge/Licence-MIT-yellow.svg)](https://opensource.org/licenses/MIT "MIT License")
[![licence: OGL3](https://img.shields.io/badge/Licence-OGL3-darkgrey "licence: Open Government Licence 3")](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
[![Tests and Linting](https://github.com/nhsengland/quick_metric/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/nhsengland/quick_metric/actions/workflows/ci.yml)
[![Documentation build with Material for MkDocs](https://img.shields.io/badge/Material_for_MkDocs-526CFE?style=for-the-badge&logo=MaterialForMkDocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)

Welcome to Quick Metric - a framework for quickly creating metrics using easy-to-edit YAML configs and reusable methods to filter, calculate, and transform data.

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
* **[Multiple Output Formats](usage/nested.md)** - Results as nested dict, pandas DataFrame, or list of records
* **[Data Filtering](api_reference/filter.md)** - Complex filtering logic with YAML configuration support
* **[Method Application](api_reference/apply_methods.md)** - Execute methods on filtered data with error handling
* **[Pipeline Integration](api_reference/pipeline.md)** - Seamless integration with oops-its-a-pipeline workflows

## Quick Start

Clone the repository:

```bash
git clone <repository-url>
cd quick_metric
```

Install Quick Metric and its dependencies:

=== "uv (recommended)"

    ```bash
    uv venv && source .venv/bin/activate
    uv pip install -e .
    ```

=== "pip"

    ```bash
    python -m venv .venv && source .venv/bin/activate
    pip install -e .
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

## Output Formats

Quick Metric supports multiple output formats to suit different use cases:

```python
# Default nested dictionary format (backward compatible)
nested_results = generate_metrics(data, config)
# Returns: {'metric_name': {'method_name': result}}

# DataFrame format (perfect for analysis and visualization)
df_results = generate_metrics(data, config, output_format="dataframe")
# Returns: pandas DataFrame with columns [metric, method, value, value_type]

# Records format (ideal for APIs and databases)
records_results = generate_metrics(data, config, output_format="records")
# Returns: [{'metric': 'metric_name', 'method': 'method_name', 'value': result}]
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
