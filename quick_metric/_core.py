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
>>> from quick_metric._core import read_metric_instructions
>>>
>>> config_path = Path('metrics.yaml')
>>> instructions = read_metric_instructions(config_path)

Execute complete workflow:

>>> import pandas as pd
>>> from quick_metric._core import interpret_metric_instructions
>>> from quick_metric._method_definitions import metric_method
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

from quick_metric._apply_methods import apply_methods
from quick_metric._filter import apply_filter
from quick_metric._method_definitions import METRICS_METHODS
from quick_metric._output_formats import OutputFormat, convert_to_format


def _normalize_method_specs(method_input):
    """
    Normalize various method specification formats into List[str | dict].

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
    List[str | dict]
        Normalized list of method specifications
    """
    if isinstance(method_input, str):
        # Single method name as string
        return [method_input]
    if isinstance(method_input, list):
        # Already a list - validate contents and return
        for item in method_input:
            if not isinstance(item, (str, dict)):
                raise ValueError(f"Method list items must be str or dict, got {type(item)}: {item}")
        return method_input
    if isinstance(method_input, dict):
        # Single method with parameters - convert to list
        return [method_input]

    raise ValueError(
        f"Method specification must be str, list, or dict, got {type(method_input)}: {method_input}"
    )


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

    logger.info(f"Processing {len(metric_instructions)} metrics on DataFrame with {len(data)} rows")

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
                raise ValueError(f"Metric '{metric_name}' instruction must be a dictionary")

            if "method" not in metric_instruction:
                logger.error("Metric missing 'method' key")
                raise ValueError(f"Metric '{metric_name}' missing required 'method' key")

            if "filter" not in metric_instruction:
                logger.error("Metric missing 'filter' key")
                raise ValueError(f"Metric '{metric_name}' missing required 'filter' key")

            # Apply filter to data
            filtered_data = apply_filter(data_df=data, filters=metric_instruction["filter"])

            logger.trace(f"Filtered to {len(filtered_data)} rows")

            # Normalize method specifications to handle various input formats
            normalized_methods = _normalize_method_specs(metric_instruction["method"])

            # Apply methods to filtered data
            with logger.contextualize(methods=normalized_methods):
                results[metric_name] = apply_methods(
                    data=filtered_data,
                    method_specs=normalized_methods,
                    metrics_methods=metrics_methods,
                )

            logger.success("Metric completed successfully")

    logger.success(f"Successfully processed all {len(results)} metrics")
    return results


def generate_metrics(
    data: pd.DataFrame,
    config: Union[Path, dict],
    metrics_methods: Optional[dict] = None,
    output_format: Union[str, OutputFormat] = "nested",
) -> Union[dict, pd.DataFrame, list[dict]]:
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
    output_format : str or OutputFormat, default "nested"
        Format for the output. Options:
        - "nested": Current dict of dicts format {'metric': {'method': result}}
        - "dataframe": Pandas DataFrame with columns [metric, method, value, value_type]
        - "records": List of dicts [{'metric': '...', 'method': '...', 'value': ...}]

    Returns
    -------
    Union[dict, pd.DataFrame, list[dict]]
        Results in the specified format. The exact type depends on output_format:
        - dict: When output_format="nested" (default)
        - pd.DataFrame: When output_format="dataframe"
        - list[dict]: When output_format="records"

    Examples
    --------
    Using a dictionary configuration (default nested format):

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
    >>> # Returns: {'category_a_count': {'count_records': 2}}

    Using DataFrame output format:

    >>> df_results = generate_metrics(data, config, output_format="dataframe")
    >>> # Returns: DataFrame with columns [metric, method, value, value_type]

    Using records output format:

    >>> records = generate_metrics(data, config, output_format="records")
    >>> # Returns: [{'metric': 'category_a_count', 'method': 'count_records', 'value': 2}]

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
        If config parameter or output_format is not a valid type.
    """
    logger.info("Starting metric generation")

    # Convert string format to enum
    if isinstance(output_format, str):
        try:
            output_format = OutputFormat(output_format)
        except ValueError as e:
            valid_formats = [f.value for f in OutputFormat]
            raise ValueError(
                f"Invalid output_format '{output_format}'. Valid options: {valid_formats}"
            ) from e

    # Handle different config input types
    if isinstance(config, Path):
        logger.debug(f"Loading configuration from file: {config}")
        metric_instructions = read_metric_instructions(config)
    elif isinstance(config, dict):
        logger.debug("Using provided dictionary configuration")
        metric_instructions = config
    else:
        logger.error(f"Invalid config type: {type(config)}")
        raise ValueError(f"Config must be a pathlib.Path object or dict, got {type(config)}")

    # Generate metrics using the existing function
    results = interpret_metric_instructions(
        data=data,
        metric_instructions=metric_instructions,
        metrics_methods=metrics_methods,
    )

    # Convert to requested format
    if output_format != OutputFormat.NESTED:
        logger.debug(f"Converting results to {output_format.value} format")
        results = convert_to_format(results, output_format)

    logger.success("Metric generation completed successfully")
    return results
