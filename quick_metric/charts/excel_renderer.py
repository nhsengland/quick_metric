"""
Excel chart rendering with xlsxwriter.

Provides functions for creating Excel charts with NHS styling.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
import pandas as pd
import xlsxwriter

from quick_metric.charts.core import NHS_COLOURS, ChartSettings, snake_to_title


def create_excel_chart(
    df: pd.DataFrame,
    output_path: Path | str,
    method_name: str,
    chart_type: str = "line",
    settings: ChartSettings | None = None,
    sheet_name: str = "Chart",
) -> Path:
    """Create an Excel file with chart and data.

    Parameters
    ----------
    df : pd.DataFrame
        Data to chart (index as x-axis, columns as series)
    output_path : Path | str
        Path for output Excel file
    method_name : str
        Method name for chart title
    chart_type : str
        Chart type: 'line', 'column', or 'bar'
    settings : ChartSettings | None
        Chart settings
    sheet_name : str
        Worksheet name

    Returns
    -------
    Path
        Path to created Excel file
    """
    settings = settings or ChartSettings()
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    workbook = xlsxwriter.Workbook(str(path))
    worksheet = workbook.add_worksheet(sheet_name)

    # Write data
    # First column is index
    worksheet.write(0, 0, df.index.name or "Index")
    for row_idx, idx_val in enumerate(df.index, start=1):
        worksheet.write(row_idx, 0, str(idx_val))

    # Data columns
    for col_idx, col_name in enumerate(df.columns, start=1):
        worksheet.write(0, col_idx, str(col_name))
        for row_idx, value in enumerate(df[col_name], start=1):
            worksheet.write(row_idx, col_idx, value)

    # Create chart
    chart_type_map = {
        "line": "line",
        "column": "column",
        "bar": "bar",
    }
    xl_chart_type = chart_type_map.get(chart_type, "line")
    chart = workbook.add_chart({"type": xl_chart_type})

    # NHS colours for series
    nhs_colors = [
        NHS_COLOURS["blue"],
        NHS_COLOURS["aqua_green"],
        NHS_COLOURS["orange"],
        NHS_COLOURS["purple"],
    ]

    # Add series
    num_rows = len(df.index)
    for col_idx, _col_name in enumerate(df.columns, start=1):
        chart.add_series(
            {
                "name": [sheet_name, 0, col_idx],
                "categories": [sheet_name, 1, 0, num_rows, 0],
                "values": [sheet_name, 1, col_idx, num_rows, col_idx],
                "line": {"color": nhs_colors[col_idx % len(nhs_colors)]},
            }
        )

    # Chart title and labels
    title = settings.title or snake_to_title(method_name)
    chart.set_title({"name": title})
    chart.set_x_axis({"name": settings.x_label})
    chart.set_y_axis({"name": settings.y_label or "Value"})

    # Size
    chart.set_size({"width": 720, "height": 432})

    # Insert chart
    data_end_col = len(df.columns) + 2
    worksheet.insert_chart(0, data_end_col, chart)

    workbook.close()
    logger.info(f"Excel chart saved: {path}")

    return path
