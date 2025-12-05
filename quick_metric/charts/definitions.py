"""
Chart type definitions and registry.

Provides:
- @chart_type decorator for registering chart types
- Base chart classes (LineChart, ColumnChart, BarChart)
- Registry functions for looking up chart types
- ChartConfig for YAML-driven configuration

Chart types are registered by name and referenced from YAML configuration.

Example YAML Configuration
--------------------------
```yaml
chart_config:
  defaults:
    enabled: true
    figsize: [10, 6]
    dpi: 150

  methods:
    monthly_compliance_rates:
      chart_type: compliance_rate  # References registered chart type
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

Example: Registering Custom Chart Types
---------------------------------------
```python
from quick_metric.charts import chart_type, ChartType, Target

@chart_type(
    name="compliance_rate",
    chart_style="line",
    y_label="Compliance Rate (%)",
    target=Target(value=0.95, label="95% Target"),
)
class ComplianceRateChart(ChartType):
    '''Line chart for compliance rate methods.'''
    pass  # Uses defaults from decorator
```
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
import threading
from typing import Any

from loguru import logger

from quick_metric.charts.core import ChartSettings, Target, snake_to_title

# =============================================================================
# Chart Type Registry
# =============================================================================

_CHART_REGISTRY: dict[str, ChartType] = {}
_REGISTRY_LOCK = threading.RLock()


def chart_type(
    name: str | None = None,
    chart_style: str = "line",
    y_label: str = "Value",
    target: Target | None = None,
):
    """Decorator to register a chart type.

    Parameters
    ----------
    name : str | None
        Name to register under. If None, uses class name in snake_case.
    chart_style : str
        Rendering style: 'line', 'column', or 'bar'
    y_label : str
        Default Y-axis label
    target : Target | None
        Default target line

    Examples
    --------
    ```python
    @chart_type(name="compliance_rate", chart_style="line", y_label="Rate (%)")
    class ComplianceRateChart(ChartType):
        '''Chart for compliance rate methods.'''
        pass
    ```
    """

    def decorator(cls):
        # Create instance with settings
        instance = cls()
        instance.chart_style = chart_style
        instance.y_label = y_label
        instance.default_target = target

        # Determine registration name
        reg_name = name or _class_to_snake(cls.__name__)

        # Register
        with _REGISTRY_LOCK:
            if reg_name in _CHART_REGISTRY:
                logger.warning(f"Chart type '{reg_name}' already registered, overwriting")
            _CHART_REGISTRY[reg_name] = instance
            logger.debug(f"Registered chart type: {reg_name}")

        return cls

    return decorator


def get_chart_type(name: str) -> ChartType:
    """Get a registered chart type by name.

    Parameters
    ----------
    name : str
        Registered name of the chart type

    Returns
    -------
    ChartType
        The registered chart type instance

    Raises
    ------
    KeyError
        If chart type not found
    """
    with _REGISTRY_LOCK:
        if name not in _CHART_REGISTRY:
            available = list(_CHART_REGISTRY.keys())
            raise KeyError(f"Chart type '{name}' not found. Available: {available}")
        return _CHART_REGISTRY[name]


def list_chart_types() -> list[str]:
    """List all registered chart type names."""
    with _REGISTRY_LOCK:
        return list(_CHART_REGISTRY.keys())


def get_all_chart_types() -> dict[str, ChartType]:
    """Get all registered chart types."""
    with _REGISTRY_LOCK:
        return dict(_CHART_REGISTRY)


def _class_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    # Remove 'Chart' suffix if present
    if name.endswith("Chart"):
        name = name[:-5]
    # Convert to snake_case
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


# =============================================================================
# Chart Type Base Class
# =============================================================================


class ChartType:
    """Base class for chart types.

    Subclass and decorate with @chart_type to register.

    Attributes
    ----------
    chart_style : str
        Rendering style: 'line', 'column', or 'bar'
    y_label : str
        Default Y-axis label
    default_target : Target | None
        Default target line
    """

    chart_style: str = "line"
    y_label: str = "Value"
    default_target: Target | None = None

    def get_title(self, method_name: str, metric_name: str | None = None) -> str:
        """Generate chart title."""
        title = snake_to_title(method_name)
        if metric_name:
            title = f"{snake_to_title(metric_name)}: {title}"
        return title

    def get_settings(self, **overrides: Any) -> ChartSettings:
        """Get chart settings with defaults and overrides."""
        settings = ChartSettings(
            y_label=self.y_label,
            target=self.default_target,
        )
        for key, value in overrides.items():
            if hasattr(settings, key) and value is not None:
                setattr(settings, key, value)
        return settings


# =============================================================================
# YAML Configuration Classes
# =============================================================================


@dataclass
class MethodChartConfig:
    """Configuration for a method's chart from YAML.

    Attributes
    ----------
    chart_type : str
        Name of registered chart type to use
    enabled : bool
        Whether to generate chart for this method
    target : Target | None
        Override default target
    y_label : str | None
        Override default Y-axis label
    include_table : bool
        Include data table below chart
    """

    chart_type: str
    enabled: bool = True
    target: Target | None = None
    y_label: str | None = None
    include_table: bool = False


@dataclass
class ChartConfig:
    """Complete chart configuration loaded from YAML.

    Attributes
    ----------
    defaults : dict
        Default settings for all charts
    methods : dict[str, MethodChartConfig]
        Per-method chart configurations
    """

    defaults: dict = field(
        default_factory=lambda: {"enabled": True, "figsize": (10, 6), "dpi": 150}
    )
    methods: dict[str, MethodChartConfig] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> ChartConfig:
        """Load from dictionary (parsed YAML)."""
        defaults = data.get("defaults", {})

        methods = {}
        for method_name, method_data in data.get("methods", {}).items():
            target = None
            if "target" in method_data:
                t = method_data["target"]
                target = Target(
                    value=t["value"],
                    label=t.get("label", "Target"),
                    color=t.get("color", "green"),
                )

            methods[method_name] = MethodChartConfig(
                chart_type=method_data.get("chart_type", "line"),
                enabled=method_data.get("enabled", True),
                target=target,
                y_label=method_data.get("y_label"),
                include_table=method_data.get("include_table", False),
            )

        return cls(defaults=defaults, methods=methods)

    def get_config_for_method(self, method_name: str) -> MethodChartConfig | None:
        """Get chart config for a method, or None if not configured."""
        return self.methods.get(method_name)


# =============================================================================
# Built-in Chart Types
# =============================================================================


@chart_type(name="line", chart_style="line", y_label="Value")
class LineChart(ChartType):
    """Generic line chart for time series and trends."""


@chart_type(name="column", chart_style="column", y_label="Count")
class ColumnChart(ChartType):
    """Generic column chart for counts and comparisons."""


@chart_type(name="bar", chart_style="bar", y_label="Value")
class BarChart(ChartType):
    """Generic horizontal bar chart."""
