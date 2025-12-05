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
import seaborn as sns

from quick_metric.charts.core import (
    get_chart_for_method,
    get_method_chart_settings,
    get_target_for_method,
    snake_to_title,
)

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

    from quick_metric.charts.definitions import BaseChart
    from quick_metric.store import MetricsStore

# =============================================================================
# NHS Colour Palette
# =============================================================================

NHS_COLOURS: dict[str, str] = {
    # Primary colours
    "blue": "#005EB8",  # NHS Blue (primary brand colour)
    "dark_blue": "#003087",  # NHS Dark Blue
    "bright_blue": "#0072CE",  # NHS Bright Blue
    "light_blue": "#41B6E6",  # NHS Light Blue
    # Secondary colours
    "green": "#009639",  # NHS Green (positive/success)
    "aqua_green": "#00A499",  # NHS Aqua Green
    "orange": "#ED8B00",  # NHS Orange (warning)
    "yellow": "#FFB81C",  # NHS Warm Yellow
    "dark_red": "#8A1538",  # NHS Dark Red (error/danger)
    "pink": "#AE2573",  # NHS Pink
    "purple": "#330072",  # NHS Purple
    # Neutral colours
    "dark_grey": "#425563",  # NHS Dark Grey
    "mid_grey": "#768692",  # NHS Mid Grey
    "pale_grey": "#E8EDEE",  # NHS Pale Grey
    "white": "#FFFFFF",
    "black": "#231F20",
}

# Colour cycle for multiple series (NHS brand compliant)
NHS_COLOUR_CYCLE: list[str] = [
    NHS_COLOURS["blue"],
    NHS_COLOURS["aqua_green"],
    NHS_COLOURS["orange"],
    NHS_COLOURS["purple"],
    NHS_COLOURS["pink"],
    NHS_COLOURS["dark_blue"],
    NHS_COLOURS["green"],
    NHS_COLOURS["yellow"],
]

# =============================================================================
# Chart Creation Functions
# =============================================================================


def create_seaborn_chart(
    df: pd.DataFrame,
    method_name: str,
    metric_name: str,
    output_path: Path | str | None = None,
    figsize: tuple[float, float] | None = None,
    include_table: bool = False,
) -> Figure | None:
    """Create a Seaborn chart for a metric method result.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with chart data (typically pivoted with dates as index)
    method_name : str
        Name of the metric method
    metric_name : str
        Name of the parent metric
    output_path : Path | str | None
        Path to save the chart. If None, chart is not saved.
    figsize : tuple[float, float] | None
        Figure size. If None, uses default from config.
    include_table : bool
        Whether to include data table below chart

    Returns
    -------
    Figure | None
        Matplotlib Figure object, or None if chart couldn't be created
    """
    chart_cls = get_chart_for_method(method_name)
    if chart_cls is None:
        logger.debug(f"No chart type for method: {method_name}")
        return None

    settings = get_method_chart_settings(method_name)
    if figsize is None:
        figsize = settings.get("figsize", (10, 6))

    # Apply NHS styling
    _apply_nhs_style()

    # Prepare data
    chart_df = _pivot_for_chart(df)
    if chart_df is None or chart_df.empty:
        logger.warning(f"No data to chart for {method_name}")
        return None

    # Create figure
    if include_table:
        fig, (chart_ax, table_ax) = plt.subplots(
            2,
            1,
            figsize=(figsize[0], figsize[1] * 1.3),
            gridspec_kw={"height_ratios": [3, 1]},
        )
    else:
        fig, chart_ax = plt.subplots(figsize=figsize)
        table_ax = None

    # Render chart
    _render_seaborn_chart(
        ax=chart_ax,
        df=chart_df,
        chart_cls=chart_cls,
        method_name=method_name,
        metric_name=metric_name,
    )

    # Add target line if configured
    _add_target_line(chart_ax, method_name)

    # Add embedded table if requested
    if include_table and table_ax is not None:
        _add_embedded_table(chart_df, table_ax, chart_cls, method_name)

    # Add NHS footer
    _add_nhs_footer(fig)

    plt.tight_layout()

    # Save if path provided
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=settings.get("dpi", 150), bbox_inches="tight")
        logger.info(f"Chart saved: {output_path}")

    return fig


def create_all_charts_for_metrics(
    metrics_store: MetricsStore,
    output_dir: Path | str,
    metric_name: str | None = None,
    include_table: bool = False,
) -> list[Path]:
    """Create charts for all methods in a MetricsStore.

    Parameters
    ----------
    metrics_store : MetricsStore
        Store containing metric results
    output_dir : Path | str
        Directory to save charts
    metric_name : str | None
        Optional filter to specific metric
    include_table : bool
        Whether to include data tables

    Returns
    -------
    list[Path]
        Paths to created chart files
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    created_charts: list[Path] = []

    for metric, method, result in metrics_store.all():
        if metric_name and metric != metric_name:
            continue

        chart_cls = get_chart_for_method(method)
        if chart_cls is None:
            continue

        # Get data as DataFrame
        data = result.data
        if isinstance(data, pd.Series):
            data = data.to_frame(name="value")
        elif not isinstance(data, pd.DataFrame):
            continue

        output_path = output_dir / f"{metric}_{method}.png"

        fig = create_seaborn_chart(
            df=data,
            method_name=method,
            metric_name=metric,
            output_path=output_path,
            include_table=include_table,
        )

        if fig is not None:
            created_charts.append(output_path)
            plt.close(fig)

    logger.info(f"Created {len(created_charts)} charts in {output_dir}")
    return created_charts


# =============================================================================
# Internal Rendering Functions
# =============================================================================


def _render_seaborn_chart(
    ax: Axes,
    df: pd.DataFrame,
    chart_cls: BaseChart,
    method_name: str,
    metric_name: str,
) -> None:
    """Render chart content to axes.

    Parameters
    ----------
    ax : Axes
        Matplotlib axes to render to
    df : pd.DataFrame
        Chart data
    chart_cls : BaseChart
        Chart type definition
    method_name : str
        Method name for titles
    metric_name : str
        Metric name for context
    """
    chart_type = chart_cls.chart_type

    # Determine if data has multiple series - use different colors for multiple
    colors = NHS_COLOUR_CYCLE[: len(df.columns)] if len(df.columns) > 1 else [NHS_COLOURS["blue"]]

    if chart_type == "line":
        for i, col in enumerate(df.columns):
            ax.plot(
                df.index,
                df[col],
                color=colors[i % len(colors)],
                linewidth=2,
                marker="o",
                markersize=4,
                label=col if len(df.columns) > 1 else None,
            )
    elif chart_type in ("column", "bar"):
        if len(df.columns) == 1:
            ax.bar(
                range(len(df)),
                df.iloc[:, 0],
                color=NHS_COLOURS["blue"],
                edgecolor=NHS_COLOURS["dark_blue"],
                linewidth=0.5,
            )
            ax.set_xticks(range(len(df)))
            ax.set_xticklabels(df.index, rotation=45, ha="right")
        else:
            # Grouped bar chart
            x = range(len(df))
            width = 0.8 / len(df.columns)
            for i, col in enumerate(df.columns):
                offset = (i - len(df.columns) / 2 + 0.5) * width
                ax.bar(
                    [xi + offset for xi in x],
                    df[col],
                    width=width,
                    color=colors[i % len(colors)],
                    label=col,
                )
            ax.set_xticks(x)
            ax.set_xticklabels(df.index, rotation=45, ha="right")

    # Set labels and title
    title = f"{snake_to_title(metric_name)}: {chart_cls.get_title(method_name)}"
    ax.set_title(title, fontsize=14, fontweight="bold", color=NHS_COLOURS["dark_blue"])
    ax.set_xlabel(chart_cls.get_x_axis_name(), fontsize=11)
    ax.set_ylabel(chart_cls.get_y_axis_name(method_name), fontsize=11)

    # Format y-axis for percentages
    if chart_cls.is_percentage(method_name):
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0%}"))
        ax.set_ylim(0, 1.05)

    # Add legend if multiple series
    if len(df.columns) > 1:
        ax.legend(loc="best", framealpha=0.9)

    # Style improvements
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3, linestyle="--")


def _add_embedded_table(
    df: pd.DataFrame,
    table_ax: Axes,
    chart_cls: BaseChart,
    method_name: str,
) -> None:
    """Add data table below chart.

    Parameters
    ----------
    df : pd.DataFrame
        Data to display
    table_ax : Axes
        Axes for table
    chart_cls : BaseChart
        Chart type for formatting
    method_name : str
        Method name for formatting decisions
    """
    table_ax.axis("off")

    # Format values
    if chart_cls.is_percentage(method_name):
        table_data = df.map(lambda x: f"{x:.1%}" if pd.notna(x) else "")
    else:
        table_data = df.map(lambda x: f"{x:,.0f}" if pd.notna(x) else "")

    # Create table
    table = table_ax.table(
        cellText=table_data.values,
        colLabels=table_data.columns,
        rowLabels=table_data.index,
        cellLoc="center",
        loc="center",
    )

    # Style table
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.5)

    # Color header row
    for key, cell in table.get_celld().items():
        if key[0] == 0:  # Header row
            cell.set_facecolor(NHS_COLOURS["blue"])
            cell.set_text_props(color="white", fontweight="bold")
        elif key[1] == -1:  # Row labels
            cell.set_facecolor(NHS_COLOURS["pale_grey"])


def _add_target_line(ax: Axes, method_name: str) -> None:
    """Add horizontal target line if configured.

    Parameters
    ----------
    ax : Axes
        Axes to add line to
    method_name : str
        Method name to look up target
    """
    target = get_target_for_method(method_name)
    if target is None:
        return

    value = target["value"]
    label = target["label"]
    color_name = target.get("color", "green")

    # Resolve color
    color = NHS_COLOURS.get(color_name, color_name)

    ax.axhline(
        y=value,
        color=color,
        linestyle="--",
        linewidth=2,
        label=label,
        zorder=5,
    )

    # Add label
    ax.text(
        ax.get_xlim()[1],
        value,
        f"  {label}",
        va="center",
        ha="left",
        fontsize=9,
        color=color,
        fontweight="bold",
    )


def _add_nhs_footer(fig: Figure) -> None:
    """Add NHS branding footer to figure.

    Parameters
    ----------
    fig : Figure
        Figure to add footer to
    """
    fig.text(
        0.99,
        0.01,
        "NHS England",
        ha="right",
        va="bottom",
        fontsize=8,
        color=NHS_COLOURS["mid_grey"],
        style="italic",
    )


def _apply_nhs_style() -> None:
    """Apply NHS styling to matplotlib/seaborn."""
    # Use seaborn whitegrid style as base
    sns.set_style("whitegrid")

    # NHS-specific overrides
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
            "axes.titleweight": "bold",
            "axes.labelcolor": NHS_COLOURS["dark_grey"],
            "text.color": NHS_COLOURS["black"],
            "axes.edgecolor": NHS_COLOURS["mid_grey"],
            "grid.color": NHS_COLOURS["pale_grey"],
            "figure.facecolor": "white",
            "axes.facecolor": "white",
        }
    )


def _pivot_for_chart(df: pd.DataFrame) -> pd.DataFrame | None:
    """Prepare DataFrame for charting.

    Attempts to pivot data into a format suitable for time series charting.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame

    Returns
    -------
    pd.DataFrame | None
        Pivoted DataFrame or None if pivot not possible
    """
    if df.empty:
        return None

    # If already in right format (index is dates/periods, columns are series)
    if len(df.columns) >= 1 and not df.index.name:
        return df

    # Try common pivot patterns
    # Pattern 1: Has 'date' or 'period' column
    date_cols = [c for c in df.columns if c.lower() in ("date", "period", "month", "year")]
    value_cols = [c for c in df.columns if c.lower() in ("value", "count", "rate", "amount")]

    if date_cols and value_cols:
        df = df.set_index(date_cols[0])
        return df[value_cols]

    # Pattern 2: Index already set appropriately
    if df.index.name or len(df.columns) <= 3:
        return df

    return df
