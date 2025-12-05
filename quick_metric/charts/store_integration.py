"""
MetricsStore integration for chart generation.

Provides functions to generate charts from MetricsStore results using
either YAML configuration or chart class definitions.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger
import pandas as pd

from quick_metric.charts.core import ChartSettings
from quick_metric.charts.definitions import ChartConfig, get_chart_type
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

    Returns
    -------
    dict[tuple[str, str], Figure]
        Dictionary of (metric, method) -> Figure for generated charts

    Examples
    --------
    ```python
    import yaml
    from quick_metric.charts import ChartConfig, charts_from_store

    # Load config from YAML
    with open("config.yaml") as f:
        config_data = yaml.safe_load(f)

    chart_config = ChartConfig.from_dict(config_data.get("chart_config", {}))

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
        # Check if method has chart config
        method_config = config.get_config_for_method(method)

        if method_config is None:
            logger.debug(f"No chart config for: {metric}.{method}")
            continue

        if not method_config.enabled:
            logger.debug(f"Chart disabled for: {metric}.{method}")
            continue

        # Get chart type from registry
        try:
            chart_type_obj = get_chart_type(method_config.chart_type)
        except KeyError:
            logger.warning(
                f"Chart type '{method_config.chart_type}' not registered for {metric}.{method}"
            )
            continue

        # Get data as DataFrame
        try:
            df = _result_to_chart_df(result)
        except ValueError as e:
            logger.warning(f"Cannot chart {metric}.{method}: {e}")
            continue

        # Build settings from chart type defaults + method config overrides
        settings = ChartSettings(
            y_label=method_config.y_label or chart_type_obj.y_label,
            target=method_config.target or chart_type_obj.default_target,
            include_table=method_config.include_table,
            figsize=tuple(config.defaults.get("figsize", (10, 6))),
            dpi=config.defaults.get("dpi", 150),
        )

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
    chart_type_name: str,
    output_path: Path | str | None = None,
    **setting_overrides,
) -> Figure:
    """Create a chart from a single MetricResult.

    Parameters
    ----------
    result : MetricResult
        Result to chart
    chart_type_name : str
        Name of registered chart type to use
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
    result = store["my_metric", "compliance_rate"]
    fig = chart_result(result, "compliance_rate", y_label="Custom Label")
    ```
    """
    chart_type_obj = get_chart_type(chart_type_name)
    df = _result_to_chart_df(result)

    settings = chart_type_obj.get_settings(**setting_overrides)

    return create_chart(
        df=df,
        method_name=result.method,
        chart_type=chart_type_obj.chart_style,
        settings=settings,
        output_path=output_path,
    )
