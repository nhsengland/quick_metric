"""
Quick Metric: A framework for quickly creating metrics using YAML configs.

This package provides a simple way to apply filters and methods to pandas
DataFrames based on YAML configuration files.
"""

from quick_metric.apply_methods import apply_method, apply_methods
from quick_metric.filter import apply_filter
from quick_metric.interpret_instructions import (
    interpret_metric_instructions,
    read_metric_instructions,
)
from quick_metric.method_definitions import METRICS_METHODS, metric_method

__version__ = "0.0.1"

__all__ = [
    "apply_method",
    "apply_methods",
    "apply_filter",
    "interpret_metric_instructions",
    "read_metric_instructions",
    "METRICS_METHODS",
    "metric_method",
]
