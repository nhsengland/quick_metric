"""
Main orchestration and workflow functionality for Quick Metric.

Provides entry points for the quick_metric framework, handling the complete
workflow from YAML configuration reading to metric result generation.

Functions
---------
read_metric_instructions : Load metric configurations from YAML files
interpret_metric_instructions : Execute complete metric workflow on data
generate_metrics : Main public API for generating metrics from data

Examples
--------
Load configuration from YAML file:

```python
from pathlib import Path
from quick_metric.core import read_metric_instructions

config_path = Path('metrics.yaml')
instructions = read_metric_instructions(config_path)
```

Execute complete workflow:

```python
import pandas as pd
from quick_metric.core import interpret_metric_instructions
from quick_metric.registry import metric_method

@metric_method
def count_records(data):
    return len(data)

data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
config = {
    'category_metrics': {
        'method': ['count_records'],
        'filter': {'category': 'A'}
    }
}
store = interpret_metric_instructions(data, config)
print(store.value('category_metrics', 'count_records'))  # 2
```

YAML Configuration Format
--------------------------
```yaml
metric_instructions:
  metric_name:
    method: ['method1', 'method2']
    filter:
      column_name: value
      and:
        condition1: value1
        condition2: value2
```

See Also
--------
filter : Data filtering functionality used by this module
apply_methods : Method execution functionality used by this module
method_definitions : Method registration system used by this module
"""

from collections.abc import Sequence
from functools import cache
from pathlib import Path
from typing import Optional, Union

from loguru import logger
import pandas as pd
import yaml

from quick_metric._apply_methods import apply_methods
from quick_metric._filter import apply_filter
from quick_metric.exceptions import MetricSpecificationError
from quick_metric.registry import METRICS_METHODS
from quick_metric.store import MetricsStore


def _normalize_method_specs(method_input) -> Sequence[Union[str, dict]]:
    """
    Normalize various method specification formats into Sequence[str | dict].

    Handles these input formats:
    - str: "method_name" -> ["method_name"]
    - list of str: ["method1", "method2"] -> ["method1", "method2"]
    - dict: {"method": {"param": "value"}} -> [{"method": {"param": "value"}}]
    - list of mixed: ["method1", {"method2": {"param": "value"}}] -> unchanged

    Parameters
    ----------
    method_input : str | list | dict
        Method specification in various formats

    Returns
    -------
    Sequence[str | dict]
        Normalized sequence of method specifications

    Raises
    ------
    MetricSpecificationError
        If the method specification format is invalid.
    """
    if isinstance(method_input, str):
        # Single method name as string
        return [method_input]
    if isinstance(method_input, list):
        # Already a list - validate contents and return
        for item in method_input:
            if not isinstance(item, (str, dict)):
                raise MetricSpecificationError(
                    f"Method list items must be str or dict, got {type(item)}: {item}",
                    method_spec=method_input,
                )
        return method_input
    if isinstance(method_input, dict):
        # Single method with parameters - convert to list
        return [method_input]

    raise MetricSpecificationError(
        f"Method specification must be str, list, or dict, got {type(method_input)}: "
        f"{method_input}",
        method_spec=method_input,
    )


@cache
def read_metric_instructions(metric_config_path: Path) -> dict:
    """
    Read metric_instructions dictionary from a YAML config file.

    Parameters
    ----------
    metric_config_path : Path
        Path to the YAML config file containing metric instructions.

    Returns
    -------
    Dict
        The 'metric_instructions' dictionary from the YAML file.

    Raises
    ------
    FileNotFoundError
        If the configuration file does not exist.
    MetricSpecificationError
        If the YAML file is invalid or missing metric_instructions.
    """
    logger.info(f"Reading metric configuration from {metric_config_path}")

    if not metric_config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {metric_config_path}")

    try:
        with open(metric_config_path, encoding="utf-8") as file:
            metric_configs = yaml.safe_load(file)

        if not isinstance(metric_configs, dict):
            raise MetricSpecificationError(
                "Configuration file must contain a YAML dictionary",
                method_spec=str(metric_config_path),
            )

        metric_instructions = metric_configs.get("metric_instructions", {})

        if not metric_instructions:
            logger.warning("No 'metric_instructions' found in configuration file")
        else:
            logger.success(f"Loaded {len(metric_instructions)} metric configurations")

        return metric_instructions

    except yaml.YAMLError as e:
        raise MetricSpecificationError(
            f"Invalid YAML in configuration file: {e}", method_spec=str(metric_config_path)
        ) from e


def interpret_metric_instructions(
    data: pd.DataFrame,
    metric_instructions: dict,
    metrics_methods: Optional[dict] = None,
) -> MetricsStore:
    """
    Apply filters and methods from metric instructions to a DataFrame.

    Creates MetricResult objects directly and returns them in a MetricsStore
    with proper dimension indexing.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to be processed.
    metric_instructions : Dict
        Dictionary containing the metrics and their filter/method conditions.
    metrics_methods : Dict, optional
        Dictionary of available methods. Defaults to METRICS_METHODS.

    Returns
    -------
    MetricsStore
        Store containing typed MetricResult objects with dimension indexing.

    Raises
    ------
    MetricSpecificationError
        If metric instructions are invalid or missing required keys.
    """
    if metrics_methods is None:
        metrics_methods = METRICS_METHODS

    logger.info(f"Processing {len(metric_instructions)} metrics on DataFrame with {len(data)} rows")

    # Basic validation
    if not isinstance(metric_instructions, dict):
        raise MetricSpecificationError(
            "metric_instructions must be a dictionary", method_spec=metric_instructions
        )

    if data.empty:
        logger.warning("Input DataFrame is empty")

    store = MetricsStore()

    for metric_name, metric_instruction in metric_instructions.items():
        with logger.contextualize(metric=metric_name):
            logger.trace("Processing metric")

            # Validate metric instruction structure
            if not isinstance(metric_instruction, dict):
                raise MetricSpecificationError(
                    f"Metric '{metric_name}' instruction must be a dictionary",
                    method_spec=metric_instruction,
                )

            if "method" not in metric_instruction:
                raise MetricSpecificationError(
                    f"Metric '{metric_name}' missing required 'method' key",
                    method_spec=metric_instruction,
                )

            if "filter" not in metric_instruction:
                raise MetricSpecificationError(
                    f"Metric '{metric_name}' missing required 'filter' key",
                    method_spec=metric_instruction,
                )

            # Apply filter to data
            filtered_data = apply_filter(data_df=data, filters=metric_instruction["filter"])

            logger.trace(f"Filtered to {len(filtered_data)} rows")

            # Normalize method specifications to handle various input formats
            normalized_methods = _normalize_method_specs(metric_instruction["method"])

            # Apply methods to filtered data and add results directly to store
            with logger.contextualize(methods=normalized_methods):
                apply_methods(
                    data=filtered_data,
                    method_specs=normalized_methods,
                    metrics_methods=metrics_methods,
                    store=store,
                    metric_name=metric_name,
                )

            logger.success("Metric completed successfully")

    logger.success(f"Successfully processed all {len(metric_instructions)} metrics")
    return store


def generate_metrics(
    data: pd.DataFrame,
    config: Union[Path, dict],
    metrics_methods: Optional[dict] = None,
) -> MetricsStore:
    """
    Generate metrics from data and return a MetricsStore.

    Dimensions are intrinsic to the data returned by methods - they are NOT
    passed as global parameters.

    This is the primary entry point for the quick_metric framework. It executes
    metric methods on filtered data and returns a MetricsStore containing typed
    results (Scalar, Series, DataFrame).

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to process
    config : Path or dict
        Path to a YAML configuration file or a dictionary of metric instructions.

        Expected dict structure:
        ```python
        {
            'metric_name': {
                'method': ['method1', 'method2'],  # or single string
                'filter': {...}  # filter specification
            }
        }
        ```
    metrics_methods : dict, optional
        Custom metrics methods registry. If None, uses the global registry.

    Returns
    -------
    MetricsStore
        Store containing all metric results as typed MetricResult objects.
        Results contain dimensions intrinsic to the data structure:
        - ScalarResult: no dimensions
        - SeriesResult: one dimension (the index)
        - DataFrameResult: N dimensions (all columns except value_column)

    Examples
    --------
    Using a dictionary configuration:

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

    store = generate_metrics(data, config)

    # Access the value
    count = store.value('category_a_count', 'count_records')  # 2

    # Or get the result object
    result = store['category_a_count', 'count_records']
    print(result.data)  # 2
    ```

    Working with dimensional data:

    ```python
    @metric_method
    def count_by_category(data):
        # Returns Series with category as dimension
        return data.groupby('category').size()

    store = generate_metrics(data, {'counts': {'method': ['count_by_category']}})

    result = store['counts', 'count_by_category']
    print(result.dimensions())  # ['category']
    print(result.data)  # Series with category index
    ```

    Export to different formats:

    ```python
    # As nested dict
    nested = store.to_nested_dict()
    # {'category_a_count': {'count_records': 2}}

    # As flat DataFrame
    df = store.to_dataframe()
    # metric            method          value  date     site
    # category_a_count  count_records   2      2025-01  R0A
    ```

    Using a YAML file:

    ```python
    from pathlib import Path
    config_path = Path('my_metrics.yaml')
    store = generate_metrics(data, config_path)
    ```

    Raises
    ------
    FileNotFoundError
        If the config path does not exist.
    MetricSpecificationError
        If a YAML file doesn't contain 'metric_instructions' key or is invalid.
        If config parameter is not a valid type.
    """
    logger.info("Starting metric generation")

    # Handle different config input types
    if isinstance(config, Path):
        logger.debug(f"Loading configuration from file: {config}")
        metric_instructions = read_metric_instructions(config)
    elif isinstance(config, dict):
        logger.debug("Using provided dictionary configuration")
        metric_instructions = config
    else:
        raise MetricSpecificationError(
            f"Config must be a pathlib.Path object or dict, got {type(config)}", method_spec=config
        )

    # Generate metrics and return MetricsStore directly
    store = interpret_metric_instructions(
        data=data,
        metric_instructions=metric_instructions,
        metrics_methods=metrics_methods,
    )

    logger.success(f"Generated {len(store)} metric results")
    return store
