# Contributing to Quick Metric

We welcome contributions to Quick Metric! This guide outlines our development practices and how to contribute effectively.

## Development Philosophy

Quick Metric follows modern Python development practices to ensure code quality, maintainability, and reliability.

## Code Standards

### Formatting and Linting

- **Tool**: `ruff` for both formatting and linting
- **Configuration**: Defined in `pyproject.toml`
- **Line Length**: 100 characters
- **Import Sorting**: Handled by ruff's isort integration

```bash
uv run ruff format quick_metric/ tests/ # (1)!
uv run ruff check quick_metric/ tests/ # (2)!
```

1. Format code automatically according to project standards
2. Check for linting issues and code quality problems

### Documentation

- **Docstring Format**: NumPy-style docstrings
- **Documentation Tool**: `mkdocs` with Material theme
- **API Documentation**: Auto-generated with `mkdocstrings`

Example docstring format:

```python
def calculate_metric(data: pd.DataFrame, method: str) -> float:
    """Calculate a metric using the specified method.
    
    Parameters
    ----------
    data : pd.DataFrame
        The input DataFrame containing the data to process.
    method : str
        The name of the calculation method to apply.
        
    Returns
    -------
    float
        The calculated metric value.
        
    Raises
    ------
    ValueError
        If the specified method is not recognized.
    """
```

## Testing Standards

### Testing Framework

- **Tool**: `pytest` with coverage reporting
- **Structure**: Class-based test organization with separate `unit/` and `e2e/` test directories
- **Coverage**: Aim for >90% test coverage
- **Fixtures**: Defined in `conftest.py` files

### Test Organization

```text
tests/
├── __init__.py
├── conftest.py          # Shared fixtures
├── unit/                # Unit tests (fast, isolated)
│   ├── test_core.py
│   ├── test_filters.py
│   └── ...
└── e2e/                 # End-to-end tests (integration)
    ├── test_pipeline.py
    └── ...
```

### Running Tests

```bash
uv run python -m pytest # (1)!
uv run python -m pytest --cov=quick_metric # (2)!
uv run python -m pytest tests/unit/ # (3)!
uv run python -m pytest tests/e2e/ # (4)!
```

1. Run all tests in the project
2. Run tests with coverage reporting
3. Run only unit tests (fast, isolated tests)
4. Run only end-to-end tests (integration tests)

### Test Organization Pattern

Tests are organized using classes, with each function being tested having its own class:

```python
class TestGenerateMetrics:
    """Test cases for the generate_metrics function."""

    def test_generate_metrics_with_valid_config(self):
        """Test generate_metrics with a valid configuration."""
        
    def test_generate_metrics_raises_error_with_invalid_method(self):
        """Test generate_metrics raises error for invalid method."""

class TestFilterData:
    """Test cases for the filter_data function."""
    
    def test_filter_data_with_simple_condition(self):
        """Test filter_data with a simple condition."""
```

### Test Naming Convention

- **Test files**: `test_<module_name>.py`
- **Test classes**: `Test<FunctionName>` (one class per function being tested)
- **Test methods**: `test_<function_being_tested>_<scenario>`

## Dependency Management

### Tool

- **Primary**: `uv` for fast dependency resolution and virtual environment management
- **Fallback**: `pip` is supported but `uv` is preferred

### Adding Dependencies

1. Add to `pyproject.toml` in the appropriate section:
   - `dependencies` for runtime dependencies
   - `dev` for development dependencies
   - `docs` for documentation dependencies

2. Update the lock file:

```bash
# Update dependency lock file after changes
uv lock
```

## Pull Request Process

1. **Fork and Clone**: Fork the repository and clone your fork
2. **Branch**: Create a feature branch from `main`
3. **Develop**: Make your changes following the code standards
4. **Test**: Ensure all tests pass and add new tests for your changes
5. **Format**: Run `ruff` formatting and linting
6. **Documentation**: Update documentation if needed
7. **Commit**: Use clear, descriptive commit messages
8. **Pull Request**: Create a PR with a clear description

### Commit Message Format

```text
type(scope): brief description

Longer explanation if needed

- Bullet points for multiple changes
- Reference issues: Fixes #123
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

## Development Workflow

### Setup

```bash
git clone https://github.com/your-username/quick_metric.git # (1)!
cd quick_metric # (2)!
uv venv # (3)!
source .venv/bin/activate # (4)!
uv pip install -e ".[dev,docs]" # (5)!
uv pip install pre-commit # (6)!
uv run pre-commit install # (7)!
```

1. Clone your fork of the repository
2. Navigate to the project directory
3. Create a virtual environment using uv
4. Activate the virtual environment (On Windows: `.venv\Scripts\activate`)
5. Install the package in development mode with all dependencies
6. Install pre-commit for code quality hooks
7. Set up pre-commit hooks to run automatically on commits

### Pre-commit Checks

Before committing, run these quality checks:

```bash
uv run ruff format quick_metric/ tests/ # (1)!
uv run ruff check quick_metric/ tests/ # (2)!
uv run python -m pytest --cov=quick_metric # (3)!
make lint test # (4)!
```

1. Format code automatically
2. Check for linting and code quality issues
3. Run tests with coverage reporting
4. Alternative: Use Makefile commands for all checks

### Pre-commit Hooks

We use pre-commit hooks to automatically check code quality:

- **ruff**: Formatting and linting
- **ruff-format**: Code formatting

The hooks run automatically on `git commit` and will prevent commits that don't meet our standards.

## Getting Help

- **Issues**: Use GitHub Issues for bug reports and feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Documentation**: Check the docs at [project documentation site]

Thank you for contributing to Quick Metric! 🚀
