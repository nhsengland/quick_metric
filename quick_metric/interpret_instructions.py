from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import yaml

from quick_metric.apply_methods import apply_methods
from quick_metric.filter import apply_filter
from quick_metric.method_definitions import METRICS_METHODS


def read_metric_instructions(metric_config_path: Path) -> Dict:
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
    with open(metric_config_path, "r", encoding="utf-8") as file:
        metric_configs = yaml.safe_load(file)
        metric_instructions = metric_configs.get("metric_instructions", {})
    return metric_instructions


def interpret_metric_instructions(
    data: pd.DataFrame,
    metric_instructions: Dict,
    metrics_methods: Optional[Dict] = None,
) -> Dict:
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

    results = {}

    for metric_name, metric_instruction in metric_instructions.items():
        # Apply filter to data
        filtered_data = apply_filter(
            data_df=data, filters=metric_instruction["filter"]
        )

        # Apply methods to filtered data
        results[metric_name] = apply_methods(
            data=filtered_data,
            method_names=metric_instruction["method"],
            metrics_methods=metrics_methods,
        )

    return results
