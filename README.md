# Quick Metric

A framework for quickly creating metrics using easy-to-edit YAML configs and reusable methods to filter, calculate, and transform data.

## Overview

Quick Metric allows you to:

- Define metric calculation methods using a simple decorator
- Configure complex data filtering and method application via YAML files
- Apply multiple metrics to pandas DataFrames in a consistent, reproducible way
- Keep your metric definitions separate from your data processing logic

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd quick_metric

# Create a virtual environment using uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install the package in development mode
uv pip install -e .

# For development with additional tools
uv pip install -e ".[dev]"
```

## Quick Start

### 1. Define Custom Metric Methods

```python
from quick_metric import metric_method

@metric_method
def count_records(data):
    """Count the number of records."""
    return len(data)

@metric_method
def mean_value(data, column='value'):
    """Calculate mean of a column."""
    return data[column].mean() if column in data.columns else 0.0
```

### 2. Create a YAML Configuration

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
      disease_type: Rare Disease
```

### 3. Apply Metrics to Your Data

```python
import pandas as pd
from pathlib import Path
from quick_metric import read_metric_instructions, interpret_metric_instructions

# Load your data
data = pd.DataFrame({
    'disease_type': ['Cancer', 'Cancer', 'Rare Disease', 'Cancer'],
    'status': ['Active', 'Inactive', 'Active', 'Active'],
    'remove': ['Keep', 'Keep', 'Remove', 'Keep'],
    'value': [10, 20, 30, 40]
})

# Load configuration
config_path = Path('config/metrics.yaml')
instructions = read_metric_instructions(config_path)

# Apply metrics
results = interpret_metric_instructions(data, instructions)

# Access results
print(results['cancer_metrics']['count_records'])  # Number of matching records
print(results['cancer_metrics']['mean_value'])     # Mean of 'value' column
```

## Configuration Format

The YAML configuration uses the following structure:

```yaml
metric_instructions:
  metric_name:
    method: ['method1', 'method2']  # List of methods to apply
    filter:                         # Filter conditions
      and:                         # All conditions must be true
        column_name: value
        column_name: [value1, value2]  # isin condition
        not:                       # Negation
          column_name: value
      or:                          # Any condition must be true
        column_name: value
```

### Filter Operators

- **Simple equality**: `column_name: value`
- **List membership**: `column_name: [value1, value2]`
- **Negation**: `not: {condition}`
- **Logical AND**: `and: {condition1, condition2, ...}`
- **Logical OR**: `or: {condition1, condition2, ...}`
- **Comparisons**:
    - `greater than: value`
    - `less than: value`
    - `greater than equal: value`
    - `less than equal: value`
- **Set operations**:
    - `in: [value1, value2]`
    - `not in: [value1, value2]`
    - `is: value`

### Advanced Filtering Example

```yaml
metric_instructions:
  complex_analysis:
    method: ['count_records', 'mean_value']
    filter:
      and:
        # Must be cancer patients
        disease_type: Cancer
        # Age between 18 and 65
        age:
          greater than equal: 18
        age:
          less than equal: 65
        # In specific locations
        location:
          in: ['London', 'Manchester', 'Birmingham']
        # Not marked for removal
        not:
          status: 'Removed'
        # Either high priority OR recent
        or:
          priority: 'High'
          date_added:
            greater than: '2024-01-01'
```

## Built-in Methods

The package includes several built-in methods that are ready to use:

- **`count_records(data)`**: Count number of records in the DataFrame
- **`mean_value(data, column='value')`**: Calculate mean of a specified column (defaults to 'value')
- **`sum_values(data, column='value')`**: Calculate sum of a specified column (defaults to 'value')
- **`describe_data(data)`**: Return descriptive statistics for the DataFrame

### Creating Custom Methods

You can easily create custom methods using the `@metric_method` decorator:

```python
from quick_metric import metric_method

@metric_method
def median_age(data):
    """Calculate median age."""
    return data['age'].median() if 'age' in data.columns else 0

@metric_method
def percentage_active(data):
    """Calculate percentage of active records."""
    if len(data) == 0:
        return 0.0
    active_count = len(data[data['status'] == 'Active'])
    return (active_count / len(data)) * 100
```

## Development

### Running Tests

We use Tox for testing across multiple Python versions and environments:

```bash
# Run tests on current Python version
tox -e py

# Run all tests across Python 3.10, 3.11, and 3.12
tox

# Run with coverage
tox -e coverage

# Run specific environments
tox -e py312,lint
```

For direct pytest usage (development):

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=quick_metric
```

### Code Quality

Using Tox (recommended):

```bash
# Format code
tox -e format

# Lint code  
tox -e lint

# Fix auto-fixable issues
tox -e fix
```

Direct usage:

```bash
# Format code
uv run ruff format

# Lint code
uv run ruff check

# Fix auto-fixable issues
uv run ruff check --fix
```

### Pre-commit Hooks (Optional)

To automatically run code quality checks before commits:

```bash
# Install pre-commit
uv pip install pre-commit

# Install the hooks  
pre-commit install

# Run on all files (optional)
pre-commit run --all-files
```

## Error Handling

The package provides clear error messages for common issues:

```python
from quick_metric.apply_methods import MetricsMethodNotFoundError

try:
    results = interpret_metric_instructions(data, instructions)
except MetricsMethodNotFoundError as e:
    print(f"Method not found: {e}")
    # Lists available methods in the error message
```

Common issues and solutions:

- **Method not found**: Ensure your custom methods are decorated with `@metric_method`
- **Column not found**: Check that your filter conditions reference existing columns
- **YAML syntax errors**: Validate your YAML configuration file
- **Empty results**: Verify that your filters don't exclude all data

## Architecture

The package consists of four main modules:

1. **`filter.py`**: Handles complex data filtering based on YAML conditions
2. **`method_definitions.py`**: Contains the decorator and method registry
3. **`apply_methods.py`**: Applies registered methods to filtered data
4. **`interpret_instructions.py`**: Orchestrates the entire process

### Data Flow

```text
YAML Config → Parse Instructions → Apply Filters → Execute Methods → Return Results
     ↓              ↓                   ↓              ↓              ↓
[metrics.yaml] → [dict] → [filtered DataFrame] → [method results] → [nested dict]
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass and code is formatted
5. Submit a pull request

## Project Structure

```text
├── LICENSE                     <- MIT license
├── Makefile                    <- Makefile with convenience commands
├── README.md                   <- The top-level README for developers
├── pyproject.toml              <- Project configuration with package metadata
├── uv.lock                     <- Lock file for reproducible dependencies
├── config/
│   └── example.yaml            <- Example YAML configuration
├── docs/                       <- MkDocs documentation
│   ├── mkdocs.yml
│   └── docs/
│       ├── index.md
│       └── getting-started.md
├── quick_metric/               <- Source code for the package
│   ├── __init__.py             <- Makes quick_metric a Python module
│   ├── apply_methods.py        <- Method application logic
│   ├── filter.py              <- Data filtering functionality
│   ├── interpret_instructions.py <- Main orchestration module
│   └── method_definitions.py   <- Method decorator and registry
└── tests/                      <- Test suite
    ├── __init__.py
    ├── test_data.py            <- Test fixtures and sample data
    ├── e2e/                    <- End-to-end integration tests
    │   ├── __init__.py
    │   └── test_workflows.py
    └── unit/                   <- Unit tests
        ├── __init__.py
        ├── test_apply_methods.py
        ├── test_filter_conditions.py
        ├── test_method_definitions.py
        └── test_recursive_filter.py
```

--------

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

--------

**Quick Metric** - Making data metrics simple, configurable, and maintainable! 🚀
