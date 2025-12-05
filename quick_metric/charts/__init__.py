"""
Charts module for NHS-branded visualizations.

Provides infrastructure for generating publication-quality charts
from MetricsStore results with NHS branding and styling.

Main Components
---------------
core : Chart configuration, registry, and target line functionality
definitions : Base chart class and common chart type definitions
seaborn_renderer : NHS-branded Seaborn/matplotlib chart rendering
excel_renderer : Excel chart embedding with xlsxwriter

Configuration
-------------
Charts can be configured via YAML:

.. code-block:: yaml

    chart_config:
      defaults:
        enabled: true
        include_table: false
        figsize: [10, 6]
        dpi: 150

      targets:
        monthly_compliance_rates:
          value: 0.95
          label: "95% Target"
          color: green

      methods:
        monthly_compliance_rates:
          enabled: true
          chart_type: line
          include_table: true

Examples
--------
Create a chart for a single method:

>>> from quick_metric.charts import create_seaborn_chart
>>> fig = create_seaborn_chart(
...     df=data,
...     method_name="compliance_rate",
...     metric_name="monthly_metrics",
...     output_path="chart.png"
... )

Create charts for all methods in a store:

>>> from quick_metric.charts import create_all_charts_for_metrics
>>> charts = create_all_charts_for_metrics(
...     metrics_store=store,
...     output_dir="charts/",
...     include_table=True
... )

Register a custom chart type:

>>> from quick_metric.charts import BaseChart, register_chart
>>>
>>> @register_chart(enabled=True)
... class CustomChart(BaseChart):
...     chart_type = "line"
...     display_name = "Custom Chart"
...
...     def matches(self, method_name: str) -> bool:
...         return "custom" in method_name.lower()
"""

# Core configuration and registry
from quick_metric.charts.core import (
    CHART_TARGETS,
    ChartConfig,
    ChartDefaults,
    ChartTarget,
    MethodChartConfig,
    clear_chart_target,
    get_chart_config,
    get_chart_for_method,
    get_method_chart_settings,
    get_registered_charts,
    get_target_for_method,
    load_chart_config,
    register_chart,
    set_chart_config,
    set_chart_target,
    should_create_chart,
    snake_to_title,
)

# Chart type definitions
from quick_metric.charts.definitions import (
    BacklogChart,
    BaseChart,
    ComplianceCountChart,
    ComplianceRateChart,
    MeanDaysChart,
)

# Excel renderer
from quick_metric.charts.excel_renderer import calculate_chart_rows, create_excel_chart

# Seaborn renderer
from quick_metric.charts.seaborn_renderer import (
    NHS_COLOUR_CYCLE,
    NHS_COLOURS,
    create_all_charts_for_metrics,
    create_seaborn_chart,
)

__all__ = [
    # Core - Configuration
    "ChartConfig",
    "ChartDefaults",
    "ChartTarget",
    "MethodChartConfig",
    "load_chart_config",
    "get_chart_config",
    "set_chart_config",
    # Core - Registry
    "register_chart",
    "get_registered_charts",
    "get_chart_for_method",
    "should_create_chart",
    "get_method_chart_settings",
    # Core - Targets
    "CHART_TARGETS",
    "get_target_for_method",
    "set_chart_target",
    "clear_chart_target",
    # Core - Utilities
    "snake_to_title",
    # Definitions
    "BaseChart",
    "ComplianceRateChart",
    "ComplianceCountChart",
    "MeanDaysChart",
    "BacklogChart",
    # Seaborn renderer
    "NHS_COLOURS",
    "NHS_COLOUR_CYCLE",
    "create_seaborn_chart",
    "create_all_charts_for_metrics",
    # Excel renderer
    "create_excel_chart",
    "calculate_chart_rows",
]
