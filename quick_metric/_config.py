"""
Configuration parsing and validation for quick_metric.

Handles YAML file reading and normalization of method specifications
from various input formats.
"""

from functools import cache
from pathlib import Path
from typing import Union

from loguru import logger
import yaml

from quick_metric.exceptions import MetricSpecificationError


def normalize_method_specs(method_input) -> list[Union[str, dict]]:
    """
    Normalize various method specification formats into list[str | dict].

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
    list[str | dict]
        Normalized list of method specifications

    Raises
    ------
    MetricSpecificationError
        If the method specification format is invalid.

    Examples
    --------
    >>> normalize_method_specs("count_records")
    ['count_records']

    >>> normalize_method_specs(["count_records", "sum_values"])
    ['count_records', 'sum_values']

    >>> normalize_method_specs({"count_records": {"min_value": 10}})
    [{'count_records': {'min_value': 10}}]
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
    dict
        The 'metric_instructions' dictionary from the YAML file.

    Raises
    ------
    FileNotFoundError
        If the configuration file does not exist.
    MetricSpecificationError
        If the YAML file is invalid or missing metric_instructions.

    Examples
    --------
    >>> from pathlib import Path
    >>> config_path = Path('config/metrics.yaml')
    >>> instructions = read_metric_instructions(config_path)
    >>> print(instructions.keys())
    dict_keys(['metric1', 'metric2', 'split_by'])
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
