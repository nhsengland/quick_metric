# Getting Started

This guide will help you install **Quick Metric** and set up your development environment.

## Prerequisites

| Requirement | Version | Description |
|-------------|---------|-------------|
| **Python** | 3.9+ | Required for modern type hints and dependency compatibility |
| **Git** | Latest | Version control system for cloning the repository |
| **uv** (Optional) | Latest | Fast Python package manager (recommended for dependency management) |

!!! info "Pipeline Integration"
    Quick Metric includes optional support for pipeline integration with `oops-its-a-pipeline`. This dependency is automatically installed and enables advanced workflow capabilities. All pipeline functionality is optional for standalone metric processing.

## Installation

Choose your preferred installation method:

=== "Git Installation (Recommended)"

    Install directly from the GitHub repository:

    === "uv (Recommended)"

        ```bash
        uv add git+https://github.com/nhsengland/quick_metric.git
        ```

    === "pip"

        ```bash
        pip install git+https://github.com/nhsengland/quick_metric.git
        ```

=== "PyPI"

    !!! warning "Not Yet Available"
        PyPI installation is not yet available but will be supported in future releases.

    === "pip"

        ```bash
        pip install quick-metric
        ```

    === "uv"

        ```bash
        uv add quick-metric
        ```

!!! tip "Development Installation"
    For development work, see the [Development Setup](#development-setup) section below.

!!! example "Quick Start Example"
    Here's a minimal example to verify your installation:

    ```python
    from quick_metric import metric_method, generate_metrics
    import pandas as pd

    @metric_method
    def count_records(data):
        """Count the number of records in the DataFrame."""
        return len(data)

    # Create sample data
    data = pd.DataFrame({
        'category': ['A', 'B', 'A', 'C'],
        'value': [10, 20, 30, 40]
    })

    # Define configuration
    config = {
        'basic_count': {
            'method': ['count_records'],
            'filter': {}
        }
    }

    # Generate metrics
    results = generate_metrics(data, config)
    print(results['basic_count']['count_records'])  # Should print: 4
    ```

    If this example runs successfully, you're ready to explore the [Usage Guide](usage/index.md)!

## Development Setup

For contributing to Quick Metric or working with the source code:

=== "uv (Recommended)"

    ```bash
    git clone https://github.com/nhsengland/quick_metric.git # (1)!
    cd quick_metric # (2)!
    uv venv # (3)!
    source .venv/bin/activate # (4)!
    uv pip install -e ".[dev,docs]" # (5)!
    uv pip install pre-commit # (6)!
    uv run pre-commit install # (7)!
    ```

    1. Clone the repository from GitHub
    2. Navigate to the project directory
    3. Create a virtual environment using uv
    4. Activate the virtual environment (On Windows: `.venv\Scripts\activate`)
    5. Install in development mode with all dependencies
    6. Install pre-commit for code quality hooks
    7. Set up pre-commit hooks to run automatically on commits

=== "pip"

    ```bash
    git clone https://github.com/nhsengland/quick_metric.git # (1)!
    cd quick_metric # (2)!
    python -m venv .venv # (3)!
    source .venv/bin/activate # (4)!
    pip install -e ".[dev,docs]" # (5)!
    pip install pre-commit # (6)!
    pre-commit install # (7)!
    ```

    1. Clone the repository from GitHub
    2. Navigate to the project directory
    3. Create a virtual environment using Python's built-in venv
    4. Activate the virtual environment (On Windows: `.venv\Scripts\activate`)
    5. Install in development mode with all dependencies
    6. Install pre-commit for code quality hooks
    7. Set up pre-commit hooks to run automatically on commits

!!! note "Contribution Guidelines"
    This project follows modern Python development practices:
    
    - **Linting & Formatting**: We use `ruff` for code formatting and linting
    - **Testing**: `pytest` with structured unit and end-to-end tests
    - **Dependency Management**: `uv` is preferred for faster dependency resolution
    - **Documentation**: `mkdocs` with Material theme
    - **Docstrings**: NumPy-style docstring format
    
    For detailed contribution guidelines, see our [Contributing Guide](contributing.md).

!!! tip "Pre-commit Hooks"
    We strongly recommend setting up pre-commit hooks during development. These automatically run code quality checks before each commit, preventing issues early in the development process. Pre-commit hooks are included in both development setup methods above.

### Development Workflow

#### Testing

```bash
uv run python -m pytest # (1)!
uv run python -m pytest tests/unit/test_output_formats.py # (2)!
uv run python -m pytest --cov=quick_metric # (3)!
uv run python -m pytest --cov=quick_metric --cov-report=html # (4)!
```

1. Run all tests in the project
2. Run a specific test file
3. Run tests with coverage reporting
4. Generate an HTML coverage report for detailed analysis

#### Code Quality

```bash
uv run ruff format quick_metric/ tests/ # (1)!
uv run ruff check quick_metric/ tests/ # (2)!
make lint # (3)!
```

1. Format code automatically according to project standards
2. Check for linting issues and code quality problems
3. Run all quality checks using the Makefile (if available)

#### Documentation

```bash
uv run mkdocs serve # (1)!
uv run mkdocs build # (2)!
uv run mkdocs gh-deploy # (3)!
```

1. Serve documentation locally with live reload for development
2. Build static documentation files for deployment
3. Deploy to GitHub Pages (maintainers only)

## Next Steps

Now that you have Quick Metric installed, explore these resources:

- **[Usage Guide](usage/index.md)** - Comprehensive guide to all Quick Metric features
- **[API Reference](api_reference/index.md)** - Detailed API documentation
- **[Configuration Guide](configuration.md)** - Learn how to write effective YAML configurations
