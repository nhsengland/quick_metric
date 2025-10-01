"""
Main orchestration and workflow functionality for Quick Metric.

This module provides the primary entry points for the quick_metric framework,
handling the complete workflow from YAML configuration reading to metric
result generation. It coordinates between the filtering, method application,
and configuration parsing components.

The module serves as the main interface for users, providing high-level
functions that abstract away the complexity of the underlying filtering
and method application processes.

Functions
---------
read_metric_instructions : Load metric configurations from YAML files
interpret_metric_instructions : Execute complete metric workflow on data

Workflow
--------
1. Load YAML configuration specifying metrics, filters, and methods
2. For each metric specification:
   - Apply filters to subset the input DataFrame
   - Execute specified methods on the filtered data
   - Collect results in a structured dictionary
3. Return comprehensive results for all metrics

Examples
--------
Load configuration from YAML file:

>>> from pathlib import Path
>>> from quick_metric.core import read_metric_instructions
>>>
>>> config_path = Path('metrics.yaml')
>>> instructions = read_metric_instructions(config_path)

Execute complete workflow:

>>> import pandas as pd
>>> from quick_metric.core import interpret_metric_instructions
>>> from quick_metric.method_definitions import metric_method
>>>
>>> @metric_method
... def count_records(data):
...     return len(data)
>>>
>>> data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
>>> config = {
...     'category_metrics': {
...         'method': ['count_records'],
...         'filter': {'category': 'A'}
...     }
... }
>>> results = interpret_metric_instructions(data, config)
>>> print(results['category_metrics']['count_records'])
2

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

from pathlib import Path
from typing import Optional, Union

from loguru import logger
import pandas as pd
import yaml

from quick_metric.apply_methods import apply_methods
from quick_metric.filter import apply_filter
from quick_metric.method_definitions import METRICS_METHODS


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
    """
    logger.info(f"Reading metric configuration from {metric_config_path}")

    if not metric_config_path.exists():
        logger.error(f"Configuration file not found: {metric_config_path}")
        raise FileNotFoundError(f"Configuration file not found: {metric_config_path}")

    try:
        with open(metric_config_path, encoding="utf-8") as file:
            metric_configs = yaml.safe_load(file)

        if not isinstance(metric_configs, dict):
            logger.error("Configuration file must contain a YAML dictionary")
            raise ValueError("Configuration file must contain a YAML dictionary")

        metric_instructions = metric_configs.get("metric_instructions", {})

        if not metric_instructions:
            logger.warning("No 'metric_instructions' found in configuration file")
        else:
            logger.success(f"Loaded {len(metric_instructions)} metric configurations")

        return metric_instructions

    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in configuration file: {e}")
        raise ValueError(f"Invalid YAML in configuration file: {e}") from e


def interpret_metric_instructions(
    data: pd.DataFrame,
    metric_instructions: dict,
    metrics_methods: Optional[dict] = None,
) -> dict:
    """
    Apply filters and methods from metric instructions to a DataFrame.

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
    Dict
        Dictionary with metric names as keys and method results as values.
    """
    if metrics_methods is None:
        metrics_methods = METRICS_METHODS

    logger.info(
        f"Processing {len(metric_instructions)} metrics on DataFrame "
        f"with {len(data)} rows"
    )

    # Basic validation
    if not isinstance(metric_instructions, dict):
        logger.error("metric_instructions must be a dictionary")
        raise ValueError("metric_instructions must be a dictionary")

    if data.empty:
        logger.warning("Input DataFrame is empty")

    results = {}

    for metric_name, metric_instruction in metric_instructions.items():
        with logger.contextualize(metric=metric_name):
            logger.trace("Processing metric")

            # Validate metric instruction structure
            if not isinstance(metric_instruction, dict):
                logger.error("Metric instruction must be a dict")
                raise ValueError(
                    f"Metric '{metric_name}' instruction must be a dictionary"
                )

            if "method" not in metric_instruction:
                logger.error("Metric missing 'method' key")
                raise ValueError(
                    f"Metric '{metric_name}' missing required 'method' key"
                )

            if "filter" not in metric_instruction:
                logger.error("Metric missing 'filter' key")
                raise ValueError(
                    f"Metric '{metric_name}' missing required 'filter' key"
                )

            # Apply filter to data
            filtered_data = apply_filter(
                data_df=data, filters=metric_instruction["filter"]
            )

            logger.trace(f"Filtered to {len(filtered_data)} rows")

            # Apply methods to filtered data
            with logger.contextualize(methods=metric_instruction["method"]):
                results[metric_name] = apply_methods(
                    data=filtered_data,
                    method_names=metric_instruction["method"],
                    metrics_methods=metrics_methods,
                )

            logger.success("Metric completed successfully")

    logger.success(f"Successfully processed all {len(results)} metrics")
    return results


def generate_metrics(
    data: pd.DataFrame,
    config: Union[Path, dict],
    metrics_methods: Optional[dict] = None,
) -> dict:
    """
    Generate metrics from data using configuration (main entry point).

    This is the primary entry point for the quick_metric framework. It provides
    a simple interface for generating metrics from pandas DataFrames using
    either YAML configuration files or dictionary configurations.

    Parameters
    ----------
    data : pd.DataFrame
        The DataFrame to process and generate metrics from.
    config : Path or Dict
        Either a Path object pointing to a YAML configuration file or a
        dictionary containing metric instructions. If a Path, the YAML file
        should contain a 'metric_instructions' key with the configuration.
    metrics_methods : Dict, optional
        Dictionary of available methods. If None, uses the default registered
        methods from METRICS_METHODS.

    Returns
    -------
    Dict
        Dictionary with metric names as keys and their calculated results
        as values. Each metric will contain the results of all methods
        applied to the filtered data.

    Examples
    --------
    Using a dictionary configuration:

    >>> import pandas as pd
    >>> from quick_metric import generate_metrics, metric_method
    >>>
    >>> @metric_method
    ... def count_records(data):
    ...     return len(data)
    >>>
    >>> data = pd.DataFrame({'category': ['A', 'B', 'A'], 'value': [1, 2, 3]})
    >>> config = {
    ...     'category_a_count': {
    ...         'method': ['count_records'],
    ...         'filter': {'category': 'A'}
    ...     }
    ... }
    >>> results = generate_metrics(data, config)

    Using a YAML file:

    >>> from pathlib import Path
    >>> config_path = Path('my_metrics.yaml')
    >>> results = generate_metrics(data, config_path)

    Raises
    ------
    FileNotFoundError
        If the config path does not exist.
    KeyError
        If a YAML file doesn't contain 'metric_instructions' key.
    ValueError
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
        logger.error(f"Invalid config type: {type(config)}")
        raise ValueError(
            f"Config must be a pathlib.Path object or dict, got {type(config)}"
        )

    # Generate metrics using the existing function
    results = interpret_metric_instructions(
        data=data,
        metric_instructions=metric_instructions,
        metrics_methods=metrics_methods,
    )

    logger.success("Metric generation completed successfully")
    return results
