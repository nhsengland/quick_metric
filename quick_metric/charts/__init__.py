"""
NHS-branded chart generation for quick_metric.

This module provides base chart types and rendering functions that can be
extended by consuming pipelines for domain-specific visualizations.

Quick Start
-----------
```python
from quick_metric.charts import create_chart, ChartSettings, Target

# Simple chart from DataFrame
fig = create_chart(
    df=result_df,
    method_name="compliance_rate",
    chart_type="line",
)

# With settings
settings = ChartSettings(
    target=Target(value=0.95, label="95% Target"),
    y_label="Compliance Rate (%)",
)
fig = create_chart(df, "compliance_rate", settings=settings)
```

Extending for Domain-Specific Charts
------------------------------------
```python
from quick_metric.charts import LineChart, Target

class ComplianceRateChart(LineChart):
    '''Line chart for compliance rate methods.'''

    y_label = "Compliance Rate (%)"
    default_target = Target(value=0.95, label="95% Target")

    def matches(self, method_name: str) -> bool:
        return "compliance_rate" in method_name.lower()
```

Integration with MetricsStore
-----------------------------
```python
from quick_metric.charts import charts_from_store

# Generate all charts from a store
charts = charts_from_store(
    store,
    chart_classes=[ComplianceRateChart(), CountChart()],
    output_dir="charts/",
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
    BaseChart,
    ColumnChart,
    LineChart,
)
from quick_metric.charts.excel_renderer import create_excel_chart
from quick_metric.charts.seaborn_renderer import (
    create_chart,
    create_chart_from_chart_class,
)
from quick_metric.charts.store_integration import chart_result, charts_from_store

__all__ = [
    # Core settings
    "ChartSettings",
    "Target",
    "NHS_COLOURS",
    "NHS_COLOUR_CYCLE",
    # Base chart types
    "BaseChart",
    "LineChart",
    "ColumnChart",
    "BarChart",
    # Rendering
    "create_chart",
    "create_chart_from_chart_class",
    "create_excel_chart",
    # Store integration
    "charts_from_store",
    "chart_result",
    # Utilities
    "snake_to_title",
    "get_color",
]
