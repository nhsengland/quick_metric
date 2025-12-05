"""
NHS-branded chart generation for quick_metric.

Charts are configured via YAML (bundled with metric definitions) and use
registered chart types. Auto-matching by method name is supported.

YAML Configuration (bundled with metric_instructions)
------------------------------------------------------
```yaml
# Define chart types once with anchors
chart_types:
  compliance_rate: &chart_compliance_rate
    chart_type: compliance_rate
    # target and y_label come from registered chart type

  compliance_count: &chart_compliance_count
    chart_type: column

# Reference in metric definitions
metric_instructions:
  metric_1ai:
    method: [monthly_compliance_rates, turnaround_compliance_counts]
    filter: ...
    charts:
      monthly_compliance_rates: *chart_compliance_rate
      turnaround_compliance_counts: *chart_compliance_count
```

Registering Custom Chart Types
------------------------------
```python
from quick_metric.charts import chart_type, ChartType, Target

# Class-based (primary pattern)
@chart_type(
    name="compliance_rate",
    chart_style="line",
    y_label="Compliance Rate (%)",
    target=Target(value=0.95, label="95% Target"),
)
class ComplianceRateChart(ChartType):
    '''Registered as "compliance_rate" for YAML reference.'''
    pass

# Function-based (alternative, like @metric_method)
@chart_type(name="simple_line", chart_style="line", y_label="Value")
def simple_line_chart():
    pass
```

Auto-Matching
-------------
When no chart_type is specified, the system tries to match the method name
to a registered chart type:

```python
# Method 'monthly_compliance_rates' will auto-match to 'compliance_rate'
# if that chart type is registered and contains 'compliance_rate' in name
fig = chart_result(result)  # No chart_type needed
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
    ChartDefaults,
    ChartType,
    ColumnChart,
    LineChart,
    MethodChartConfig,
    chart_type,
    find_chart_type,
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
    "find_chart_type",
    "list_chart_types",
    "get_all_chart_types",
    # Built-in chart types
    "LineChart",
    "ColumnChart",
    "BarChart",
    # YAML configuration
    "ChartConfig",
    "ChartDefaults",
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
