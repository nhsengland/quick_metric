"""
NHS-branded Seaborn and Matplotlib chart rendering.

Provides functions for creating publication-quality charts with
NHS branding, colours, and styling guidelines.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger
import matplotlib.pyplot as plt
import pandas as pd

from quick_metric.charts.core import (
    NHS_COLOUR_CYCLE,
    NHS_COLOURS,
    ChartSettings,
    get_color,
    snake_to_title,
)

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure


def apply_nhs_style(ax: Axes, settings: ChartSettings, method_name: str) -> None:
    """Apply NHS branding and styling to a matplotlib axes.

    Parameters
    ----------
    ax : Axes
        Matplotlib axes to style
    settings : ChartSettings
        Chart settings
    method_name : str
        Method name for auto-generated title
    """
    # Title
    title = settings.title or snake_to_title(method_name)
    ax.set_title(title, fontsize=14, fontweight="bold", color=NHS_COLOURS["dark_grey"])

    # Axis labels
    ax.set_xlabel(settings.x_label, fontsize=11, color=NHS_COLOURS["dark_grey"])
    y_label = settings.y_label or "Value"
    ax.set_ylabel(y_label, fontsize=11, color=NHS_COLOURS["dark_grey"])

    # Styling
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(NHS_COLOURS["mid_grey"])
    ax.spines["bottom"].set_color(NHS_COLOURS["mid_grey"])
    ax.tick_params(colors=NHS_COLOURS["dark_grey"])

    # Grid
    ax.yaxis.grid(True, linestyle="--", alpha=0.3, color=NHS_COLOURS["mid_grey"])
    ax.set_axisbelow(True)


def add_target_line(ax: Axes, settings: ChartSettings) -> None:
    """Add target line to chart if configured.

    Parameters
    ----------
    ax : Axes
        Matplotlib axes
    settings : ChartSettings
        Chart settings containing target
    """
    if settings.target is None:
        return

    color = get_color(settings.target.color)
    ax.axhline(
        y=settings.target.value,
        color=color,
        linestyle="--",
        linewidth=2,
        label=settings.target.label,
    )


def add_footer(fig: Figure, settings: ChartSettings) -> None:
    """Add footer text to figure if configured.

    Parameters
    ----------
    fig : Figure
        Matplotlib figure
    settings : ChartSettings
        Chart settings containing footer
    """
    if settings.footer:
        fig.text(
            0.99,
            0.01,
            settings.footer,
            ha="right",
            va="bottom",
            fontsize=8,
            color=NHS_COLOURS["mid_grey"],
            style="italic",
        )


def render_line_chart(
    ax: Axes,
    df: pd.DataFrame,
    _settings: ChartSettings,
) -> None:
    """Render a line chart on the axes.

    Parameters
    ----------
    ax : Axes
        Matplotlib axes
    df : pd.DataFrame
        Data with index as x-axis, columns as series
    _settings : ChartSettings
        Chart settings (unused, for API consistency)
    """
    colors = NHS_COLOUR_CYCLE[: len(df.columns)] if len(df.columns) > 1 else [NHS_COLOURS["blue"]]

    for i, col in enumerate(df.columns):
        ax.plot(
            df.index,
            df[col],
            color=colors[i % len(colors)],
            linewidth=2,
            marker="o",
            markersize=4,
            label=str(col),
        )

    if len(df.columns) > 1:
        ax.legend(loc="upper left", frameon=False)


def render_column_chart(
    ax: Axes,
    df: pd.DataFrame,
    _settings: ChartSettings,
) -> None:
    """Render a column (vertical bar) chart on the axes.

    Parameters
    ----------
    ax : Axes
        Matplotlib axes
    df : pd.DataFrame
        Data with index as x-axis, columns as series
    _settings : ChartSettings
        Chart settings (unused, for API consistency)
    """
    colors = NHS_COLOUR_CYCLE[: len(df.columns)] if len(df.columns) > 1 else [NHS_COLOURS["blue"]]

    x = range(len(df.index))
    width = 0.8 / len(df.columns)

    for i, col in enumerate(df.columns):
        offset = (i - len(df.columns) / 2 + 0.5) * width
        ax.bar(
            [xi + offset for xi in x],
            df[col],
            width=width,
            color=colors[i % len(colors)],
            label=str(col),
        )

    ax.set_xticks(list(x))
    ax.set_xticklabels([str(label) for label in df.index], rotation=45, ha="right")

    if len(df.columns) > 1:
        ax.legend(loc="upper left", frameon=False)


def render_bar_chart(
    ax: Axes,
    df: pd.DataFrame,
    _settings: ChartSettings,
) -> None:
    """Render a bar (horizontal) chart on the axes.

    Parameters
    ----------
    ax : Axes
        Matplotlib axes
    df : pd.DataFrame
        Data with index as categories
    _settings : ChartSettings
        Chart settings (unused, for API consistency)
    """
    # For horizontal bars, use first column only
    col = df.columns[0]
    y = range(len(df.index))

    ax.barh(y, df[col], color=NHS_COLOURS["blue"], height=0.6)
    ax.set_yticks(list(y))
    ax.set_yticklabels([str(label) for label in df.index])
    ax.invert_yaxis()  # Top to bottom


def create_chart(
    df: pd.DataFrame,
    method_name: str,
    chart_type: str = "line",
    settings: ChartSettings | None = None,
    output_path: Path | str | None = None,
) -> Figure:
    """Create a chart from DataFrame data.

    Parameters
    ----------
    df : pd.DataFrame
        Data to chart (index as x-axis, columns as series)
    method_name : str
        Method name for title generation
    chart_type : str
        Chart type: 'line', 'column', or 'bar'
    settings : ChartSettings | None
        Chart settings. If None, uses defaults.
    output_path : Path | str | None
        Path to save the chart. If None, chart is not saved.

    Returns
    -------
    Figure
        Matplotlib figure
    """
    settings = settings or ChartSettings()

    # Create figure
    fig, ax = plt.subplots(figsize=settings.figsize, dpi=settings.dpi)

    # Render based on chart type
    renderers = {
        "line": render_line_chart,
        "column": render_column_chart,
        "bar": render_bar_chart,
    }

    renderer = renderers.get(chart_type, render_line_chart)
    renderer(ax, df, settings)

    # Apply styling
    apply_nhs_style(ax, settings, method_name)
    add_target_line(ax, settings)
    add_footer(fig, settings)

    # Tight layout
    fig.tight_layout()

    # Save if path provided
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=settings.dpi, bbox_inches="tight", facecolor="white")
        logger.info(f"Chart saved: {path}")

    return fig
