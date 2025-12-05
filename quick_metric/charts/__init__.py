"""
NHS-branded chart generation for quick_metric.

Charts are configured via YAML and use registered chart types.

YAML Configuration
------------------
```yaml
chart_config:
  defaults:
    enabled: true
    figsize: [10, 6]
    dpi: 150

  methods:
    monthly_compliance_rates:
      chart_type: compliance_rate  # References registered type
      include_table: true

    turnaround_compliance_counts:
      chart_type: column
      y_label: "Count"

    mean_days_over_standard:
      chart_type: line
      target:
        value: 0
        label: "On Time"
        color: green
```

Registering Custom Chart Types
------------------------------
```python
from quick_metric.charts import chart_type, ChartType, Target

@chart_type(
    name="compliance_rate",
    chart_style="line",
    y_label="Compliance Rate (%)",
    target=Target(value=0.95, label="95% Target"),
)
class ComplianceRateChart(ChartType):
    '''Registered as "compliance_rate" for YAML reference.'''
    pass
```

Direct Chart Creation
---------------------
```python
from quick_metric.charts import create_chart, ChartSettings, Target

fig = create_chart(
    df=result_df,
    method_name="compliance_rate",
    chart_type="line",
    settings=ChartSettings(
        target=Target(value=0.95, label="95% Target"),
        y_label="Compliance Rate (%)",
    ),
)
```
"""

from quick_metric.charts.core import (
    NHS_COLOUR_CYCLE,
    NHS_COLOURS,
    ChartSettings,
    Target,
    get_color,
    snake_to_title,
)
from quick_metric.charts.definitions import (
    BarChart,
    ChartConfig,
    ChartType,
    ColumnChart,
    LineChart,
    MethodChartConfig,
    chart_type,
    get_all_chart_types,
    get_chart_type,
    list_chart_types,
)
from quick_metric.charts.excel_renderer import create_excel_chart
from quick_metric.charts.seaborn_renderer import (
    create_chart,
)
from quick_metric.charts.store_integration import chart_result, charts_from_store

__all__ = [
    # Core settings
    "ChartSettings",
    "Target",
    "NHS_COLOURS",
    "NHS_COLOUR_CYCLE",
    # Chart type registration
    "chart_type",
    "ChartType",
    "get_chart_type",
    "list_chart_types",
    "get_all_chart_types",
    # Built-in chart types
    "LineChart",
    "ColumnChart",
    "BarChart",
    # YAML configuration
    "ChartConfig",
    "MethodChartConfig",
    # Rendering
    "create_chart",
    "create_excel_chart",
    # Store integration
    "charts_from_store",
    "chart_result",
    # Utilities
    "snake_to_title",
    "get_color",
]
