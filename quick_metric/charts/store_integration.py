"""
MetricsStore integration for chart generation.

Provides functions to generate charts from MetricsStore results using
either YAML configuration or chart class definitions.

Auto-Matching
-------------
When chart_type is not specified in config, the system attempts to
auto-match by method name:
1. Exact match: method name == chart type name
2. Partial match: method name contains chart type name

This allows minimal config when method names are descriptive.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger
import pandas as pd

from quick_metric.charts.core import ChartSettings
from quick_metric.charts.definitions import (
    ChartConfig,
    MethodChartConfig,
    find_chart_type,
    get_chart_type,
)
from quick_metric.charts.seaborn_renderer import create_chart
from quick_metric.results import DataFrameResult, ScalarResult, SeriesResult

if TYPE_CHECKING:
    from matplotlib.figure import Figure

    from quick_metric.store import MetricsStore


def charts_from_store(
    store: MetricsStore,
    chart_config: ChartConfig | None = None,
    output_dir: Path | str | None = None,
    file_format: str = "png",
    auto_match: bool = True,
) -> dict[tuple[str, str], Figure]:
    """Generate charts for all configured methods in a MetricsStore.

    Parameters
    ----------
    store : MetricsStore
        Store containing metric results
    chart_config : ChartConfig | None
        Chart configuration from YAML. If None, uses defaults.
    output_dir : Path | str | None
        Directory to save charts. If None, charts are not saved.
    file_format : str
        Image format for saved files (png, svg, pdf)
    auto_match : bool
        If True, attempt to auto-match method names to chart types
        when no explicit chart_type is specified

    Returns
    -------
    dict[tuple[str, str], Figure]
        Dictionary of (metric, method) -> Figure for generated charts

    Examples
    --------
    ```python
    import yaml
    from quick_metric.charts import ChartConfig, charts_from_store

    # Load config from YAML (bundled with metric_instructions)
    with open("config.yaml") as f:
        config_data = yaml.safe_load(f)

    # Extract chart config from metric_instructions
    chart_config = ChartConfig.from_metric_instructions(
        config_data.get("metric_instructions", {})
    )

    charts = charts_from_store(
        store,
        chart_config=chart_config,
        output_dir="output/charts/",
    )
    ```
    """
    config = chart_config or ChartConfig()
    output_path = Path(output_dir) if output_dir else None
    charts: dict[tuple[str, str], Figure] = {}

    for metric, method, result in store.all():
        # Get chart type - explicit config, auto-match, or skip
        chart_type_obj, method_config = _resolve_chart_type(method, config, auto_match=auto_match)

        if chart_type_obj is None:
            logger.debug(f"No chart type for: {metric}.{method}")
            continue

        if method_config and not method_config.enabled:
            logger.debug(f"Chart disabled for: {metric}.{method}")
            continue

        # Get data as DataFrame
        try:
            df = _result_to_chart_df(result)
        except ValueError as e:
            logger.warning(f"Cannot chart {metric}.{method}: {e}")
            continue

        # Build settings from chart type defaults + method config overrides
        settings = _build_settings(chart_type_obj, method_config, config)

        # Generate output path if saving
        save_path = None
        if output_path:
            save_path = output_path / f"{metric}_{method}.{file_format}"

        # Create chart
        fig = create_chart(
            df=df,
            method_name=method,
            chart_type=chart_type_obj.chart_style,
            settings=settings,
            output_path=save_path,
        )

        charts[(metric, method)] = fig
        logger.info(f"Created chart: {metric}.{method}")

    return charts


def _resolve_chart_type(
    method_name: str,
    config: ChartConfig,
    auto_match: bool = True,
) -> tuple:
    """Resolve chart type for a method.

    Priority:
    1. Explicit chart_type in method config
    2. Auto-match by method name (if enabled)
    3. None

    Returns
    -------
    tuple[ChartType | None, MethodChartConfig | None]
        Chart type and optional method config
    """

    method_config = config.get_config_for_method(method_name)

    # 1. Explicit chart_type in config
    if method_config and method_config.chart_type:
        try:
            chart_type_obj = get_chart_type(method_config.chart_type)
            return chart_type_obj, method_config
        except KeyError:
            logger.warning(
                f"Chart type '{method_config.chart_type}' not registered for {method_name}"
            )
            return None, method_config

    # 2. Auto-match by method name
    if auto_match:
        chart_type_obj = find_chart_type(method_name)
        if chart_type_obj:
            logger.debug(f"Auto-matched chart type for: {method_name}")
            return chart_type_obj, method_config

    # 3. Method config exists but no chart_type (with auto_match disabled)
    if method_config:
        # Use default 'line' if enabled
        try:
            chart_type_obj = get_chart_type("line")
            return chart_type_obj, method_config
        except KeyError:
            return None, method_config

    return None, None


def _build_settings(
    chart_type_obj,
    method_config: MethodChartConfig | None,
    config: ChartConfig,
) -> ChartSettings:
    """Build chart settings from chart type defaults and config overrides."""
    # Start with chart type defaults
    y_label = chart_type_obj.y_label
    target = chart_type_obj.default_target
    include_table = False

    # Apply method config overrides
    if method_config:
        if method_config.y_label:
            y_label = method_config.y_label
        if method_config.target:
            target = method_config.target
        include_table = method_config.include_table

    return ChartSettings(
        y_label=y_label,
        target=target,
        include_table=include_table,
        figsize=config.defaults.figsize,
        dpi=config.defaults.dpi,
    )


def _result_to_chart_df(result) -> pd.DataFrame:
    """Convert a MetricResult to a DataFrame suitable for charting.

    Parameters
    ----------
    result : MetricResult
        Result to convert

    Returns
    -------
    pd.DataFrame
        DataFrame with index as x-axis, columns as series

    Raises
    ------
    ValueError
        If result cannot be converted to chartable format
    """
    if isinstance(result, ScalarResult):
        raise ValueError("Scalar results cannot be charted directly")

    if isinstance(result, SeriesResult):
        # Series -> DataFrame with one column
        return result.data.to_frame(name=result.method)

    if isinstance(result, DataFrameResult):
        # DataFrame - return as-is or pivot if needed
        return result.data

    raise ValueError(f"Unknown result type: {type(result)}")


def chart_result(
    result,
    chart_type_name: str | None = None,
    output_path: Path | str | None = None,
    **setting_overrides,
) -> Figure:
    """Create a chart from a single MetricResult.

    Parameters
    ----------
    result : MetricResult
        Result to chart
    chart_type_name : str | None
        Name of registered chart type to use. If None, auto-match by method name.
    output_path : Path | str | None
        Path to save the chart
    **setting_overrides
        Override any chart settings

    Returns
    -------
    Figure
        Matplotlib figure

    Examples
    --------
    ```python
    # Explicit chart type
    result = store["my_metric", "compliance_rate"]
    fig = chart_result(result, "compliance_rate", y_label="Custom Label")

    # Auto-match by method name
    fig = chart_result(result)  # Will try to match 'compliance_rate'
    ```
    """
    # Resolve chart type
    if chart_type_name:
        chart_type_obj = get_chart_type(chart_type_name)
    else:
        # Auto-match by method name
        chart_type_obj = find_chart_type(result.method)
        if chart_type_obj is None:
            # Fall back to line chart
            chart_type_obj = get_chart_type("line")

    df = _result_to_chart_df(result)
    settings = chart_type_obj.get_settings(**setting_overrides)

    return create_chart(
        df=df,
        method_name=result.method,
        chart_type=chart_type_obj.chart_style,
        settings=settings,
        output_path=output_path,
    )
