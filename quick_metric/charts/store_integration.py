"""
MetricsStore integration for chart generation.

Provides functions to generate charts from MetricsStore results using
chart class definitions.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger
import pandas as pd

from quick_metric.charts.seaborn_renderer import create_chart_from_chart_class
from quick_metric.results import DataFrameResult, ScalarResult, SeriesResult

if TYPE_CHECKING:
    from matplotlib.figure import Figure

    from quick_metric.charts.definitions import BaseChart
    from quick_metric.store import MetricsStore


def charts_from_store(
    store: MetricsStore,
    chart_classes: list[BaseChart],
    output_dir: Path | str | None = None,
    file_format: str = "png",
) -> dict[tuple[str, str], Figure]:
    """Generate charts for all matching results in a MetricsStore.

    Parameters
    ----------
    store : MetricsStore
        Store containing metric results
    chart_classes : list[BaseChart]
        List of chart class instances to match against methods
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
    from quick_metric.charts import LineChart, ColumnChart, charts_from_store

    class RateChart(LineChart):
        y_label = "Rate (%)"
        def matches(self, method_name):
            return "rate" in method_name.lower()

    class CountChart(ColumnChart):
        def matches(self, method_name):
            return "count" in method_name.lower()

    charts = charts_from_store(
        store,
        chart_classes=[RateChart(), CountChart()],
        output_dir="output/charts/",
    )
    ```
    """
    output_path = Path(output_dir) if output_dir else None
    charts: dict[tuple[str, str], Figure] = {}

    for metric, method, result in store.all():
        # Find matching chart class
        matching_chart = None
        for chart_cls in chart_classes:
            if chart_cls.matches(method):
                matching_chart = chart_cls
                break

        if matching_chart is None:
            logger.debug(f"No chart class matches: {metric}.{method}")
            continue

        # Get data as DataFrame
        try:
            df = _result_to_chart_df(result)
        except ValueError as e:
            logger.warning(f"Cannot chart {metric}.{method}: {e}")
            continue

        # Generate output path if saving
        save_path = None
        if output_path:
            save_path = output_path / f"{metric}_{method}.{file_format}"

        # Create chart
        fig = create_chart_from_chart_class(
            df=df,
            method_name=method,
            chart_class=matching_chart,
            metric_name=metric,
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
    chart_class: BaseChart,
    output_path: Path | str | None = None,
    **setting_overrides,
) -> Figure:
    """Create a chart from a single MetricResult.

    Parameters
    ----------
    result : MetricResult
        Result to chart
    chart_class : BaseChart
        Chart class to use for rendering
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
    fig = chart_result(result, RateChart(), y_label="Custom Label")
    ```
    """
    df = _result_to_chart_df(result)

    return create_chart_from_chart_class(
        df=df,
        method_name=result.method,
        chart_class=chart_class,
        metric_name=result.metric,
        output_path=output_path,
        **setting_overrides,
    )
