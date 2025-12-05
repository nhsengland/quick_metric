"""
Excel chart rendering with xlsxwriter.

Provides functions for embedding charts in Excel worksheets
using the xlsxwriter library.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger
import pandas as pd

from quick_metric.charts.core import get_chart_for_method, snake_to_title

if TYPE_CHECKING:
    from xlsxwriter.workbook import Workbook
    from xlsxwriter.worksheet import Worksheet

# =============================================================================
# Excel Chart Configuration
# =============================================================================

# Default chart dimensions in Excel units
CHART_WIDTH = 720  # pixels
CHART_HEIGHT = 400  # pixels


def calculate_chart_rows(df: pd.DataFrame) -> int:
    """Calculate number of Excel rows a chart will occupy.

    Parameters
    ----------
    df : pd.DataFrame
        Data being charted (affects chart height)

    Returns
    -------
    int
        Number of rows the chart will span
    """
    # Base rows for chart + some padding
    base_rows = 20

    # Add extra rows for larger datasets
    if len(df) > 12:
        base_rows += 5

    return base_rows


def create_excel_chart(
    workbook: Workbook,
    worksheet: Worksheet,
    df: pd.DataFrame,
    method_name: str,
    row_offset: int,
    col_offset: int = 0,
    data_start_row: int | None = None,
    data_start_col: int | None = None,
) -> int:
    """Create an Excel chart for metric data.

    Parameters
    ----------
    workbook : Workbook
        xlsxwriter Workbook object
    worksheet : Worksheet
        Worksheet to add chart to
    df : pd.DataFrame
        Data to chart
    method_name : str
        Name of the metric method
    row_offset : int
        Row to insert chart at
    col_offset : int
        Column offset for chart placement
    data_start_row : int | None
        Row where data starts in worksheet. If None, assumes data is written
        immediately after headers.
    data_start_col : int | None
        Column where data starts. If None, uses col_offset.

    Returns
    -------
    int
        Number of rows consumed by the chart
    """
    chart_cls = get_chart_for_method(method_name)
    if chart_cls is None:
        logger.debug(f"No chart type registered for method: {method_name}")
        return 0

    # Determine chart type
    chart_type = chart_cls.chart_type
    excel_chart_type = _map_chart_type(chart_type)

    # Create chart
    chart = workbook.add_chart({"type": excel_chart_type})

    # Calculate data range
    if data_start_row is None:
        data_start_row = row_offset + 1  # Assume header is at row_offset
    if data_start_col is None:
        data_start_col = col_offset

    data_end_row = data_start_row + len(df) - 1
    sheet_name = worksheet.name

    # Add series for each column (except index)
    for i, col in enumerate(df.columns):
        col_num = data_start_col + i + 1  # +1 to skip index column

        # Category (X-axis) range - typically the index/first column
        categories = [sheet_name, data_start_row, data_start_col, data_end_row, data_start_col]

        # Values (Y-axis) range
        values = [sheet_name, data_start_row, col_num, data_end_row, col_num]

        series_options = {
            "name": str(col),
            "categories": categories,
            "values": values,
        }

        # Add formatting based on chart type
        if excel_chart_type == "line":
            series_options["line"] = {"width": 2.5}
            series_options["marker"] = {"type": "circle", "size": 5}

        chart.add_series(series_options)

    # Configure chart
    title = snake_to_title(method_name)
    chart.set_title({"name": title, "name_font": {"bold": True, "size": 12}})

    chart.set_x_axis(
        {
            "name": chart_cls.get_x_axis_name(),
            "name_font": {"size": 10},
        }
    )

    chart.set_y_axis(
        {
            "name": chart_cls.get_y_axis_name(method_name),
            "name_font": {"size": 10},
        }
    )

    # Format y-axis for percentages
    if chart_cls.is_percentage(method_name):
        chart.set_y_axis(
            {
                "name": chart_cls.get_y_axis_name(method_name),
                "name_font": {"size": 10},
                "num_format": "0%",
                "min": 0,
                "max": 1.05,
            }
        )

    # Set chart size
    chart.set_size({"width": CHART_WIDTH, "height": CHART_HEIGHT})

    # Add legend if multiple series
    if len(df.columns) > 1:
        chart.set_legend({"position": "bottom"})
    else:
        chart.set_legend({"none": True})

    # Insert chart into worksheet
    cell = _cell_ref(row_offset, col_offset)
    worksheet.insert_chart(cell, chart)

    return calculate_chart_rows(df)


def _map_chart_type(chart_type: str) -> str:
    """Map internal chart type to xlsxwriter chart type.

    Parameters
    ----------
    chart_type : str
        Internal chart type name

    Returns
    -------
    str
        xlsxwriter chart type
    """
    mapping = {
        "line": "line",
        "column": "column",
        "bar": "bar",
        "area": "area",
        "scatter": "scatter",
        "pie": "pie",
    }
    return mapping.get(chart_type, "line")


def _cell_ref(row: int, col: int) -> str:
    """Convert row/column numbers to Excel cell reference.

    Parameters
    ----------
    row : int
        Zero-based row number
    col : int
        Zero-based column number

    Returns
    -------
    str
        Excel cell reference (e.g., 'A1', 'B5')
    """
    col_letter = ""
    col_num = col

    while col_num >= 0:
        col_letter = chr(ord("A") + col_num % 26) + col_letter
        col_num = col_num // 26 - 1

    return f"{col_letter}{row + 1}"
