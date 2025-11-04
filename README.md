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

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd quick_metric

# Create virtual environment and install
uv venv && source .venv/bin/activate
uv pip install -e .
```

## Quick Start

```python
from quick_metric import metric_method, generate_metrics
import pandas as pd

# 1. Define custom metrics
@metric_method
def count_records(data):
    return len(data)

@metric_method
def mean_value(data, column='value'):
    return data[column].mean()

# 2. Create data and config
data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [10, 20, 30]})
config = {
    'category_a_metrics': {
        'method': ['count_records', 'mean_value'],
        'filter': {'category': 'A'}
    }
}

# 3. Generate metrics
results = generate_metrics(data, config)
print(results['category_a_metrics']['count_records'])  # 2
print(results['category_a_metrics']['mean_value'])     # 20.0
```

## Key Features

- **Simple decorator**: Register metrics with `@metric_method`
- **Flexible configuration**: Use dictionaries or YAML files
- **Multiple output formats**: Nested dict, DataFrame, records, or flat DataFrame
- **Advanced filtering**: Complex data filtering with logical operators
- **Pipeline integration**: Works with `oops-its-a-pipeline` workflows

## Documentation

📚 **[Full Documentation](https://nhsengland.github.io/quick_metric)**

- [Getting Started](https://nhsengland.github.io/quick_metric/getting_started/) - Installation and setup
- [Usage Guide](https://nhsengland.github.io/quick_metric/usage/) - Comprehensive tutorials and examples  
- [API Reference](https://nhsengland.github.io/quick_metric/api_reference/) - Complete API documentation

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

---

**Quick Metric** - Making data metrics simple, configurable, and maintainable! 🚀
