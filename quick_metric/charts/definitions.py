"""
Chart type definitions and registry.

Provides:
- @chart_type decorator for registering chart types (class or function)
- ChartType base class for class-based charts
- Registry functions for looking up chart types
- Auto-matching by method name when no chart_type specified

YAML Configuration (bundled with metric definitions)
-----------------------------------------------------
```yaml
# Define chart types once with anchors
chart_types:
  compliance_rate: &chart_compliance_rate
    chart_type: compliance_rate
    # target and y_label come from registered chart type

  compliance_count: &chart_compliance_count
    chart_type: compliance_count

  mean_days: &chart_mean_days
    chart_type: mean_days

# Reference in metric definitions
metric_instructions:
  metric_1ai:
    method: [monthly_compliance_rates, turnaround_compliance_counts]
    filter: ...
    charts:
      monthly_compliance_rates: *chart_compliance_rate
      turnaround_compliance_counts: *chart_compliance_count
```

Registering Chart Types
-----------------------
```python
# Class-based (primary pattern)
@chart_type(
    name="compliance_rate",
    chart_style="line",
    y_label="Compliance Rate (%)",
    target=Target(value=0.95, label="95% Target"),
)
class ComplianceRateChart(ChartType):
    pass

# Function-based (alternative, like @metric_method)
@chart_type(name="simple_line", chart_style="line", y_label="Value")
def simple_line_chart():
    pass
```
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
import threading
from typing import Any, Callable

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
    """Decorator to register a chart type (class or function).

    Parameters
    ----------
    name : str | None
        Name to register under. If None, uses class/function name in snake_case.
    chart_style : str
        Rendering style: 'line', 'column', or 'bar'
    y_label : str
        Default Y-axis label
    target : Target | None
        Default target line

    Examples
    --------
    ```python
    # Class-based (primary)
    @chart_type(name="compliance_rate", chart_style="line", y_label="Rate (%)")
    class ComplianceRateChart(ChartType):
        pass

    # Function-based (alternative)
    @chart_type(name="simple_line", chart_style="line")
    def simple_line_chart():
        pass
    ```
    """

    def decorator(cls_or_func: type | Callable):
        # Determine if class or function
        if isinstance(cls_or_func, type):
            # Class-based
            instance = cls_or_func()
            reg_name = name or _class_to_snake(cls_or_func.__name__)
        else:
            # Function-based - create a ChartType instance
            instance = ChartType()
            reg_name = name or cls_or_func.__name__

        # Set attributes
        instance.chart_style = chart_style
        instance.y_label = y_label
        instance.default_target = target

        # Register
        with _REGISTRY_LOCK:
            if reg_name in _CHART_REGISTRY:
                logger.warning(f"Chart type '{reg_name}' already registered, overwriting")
            _CHART_REGISTRY[reg_name] = instance
            logger.debug(f"Registered chart type: {reg_name}")

        return cls_or_func

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


def find_chart_type(method_name: str) -> ChartType | None:
    """Find a chart type that matches a method name.

    Tries exact match first, then partial matches.

    Parameters
    ----------
    method_name : str
        Method name to match

    Returns
    -------
    ChartType | None
        Matching chart type, or None if not found
    """
    with _REGISTRY_LOCK:
        # Exact match
        if method_name in _CHART_REGISTRY:
            return _CHART_REGISTRY[method_name]

        # Partial match - check if method contains chart type name
        for chart_name, chart_obj in _CHART_REGISTRY.items():
            if chart_name in method_name:
                return chart_obj

        return None


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

# Code defaults - can be overridden in YAML
_CODE_DEFAULTS = {
    "enabled": True,
    "figsize": (10, 6),
    "dpi": 150,
}


@dataclass
class MethodChartConfig:
    """Configuration for a method's chart.

    Attributes
    ----------
    chart_type : str | None
        Name of registered chart type. If None, auto-match by method name.
    enabled : bool
        Whether to generate chart for this method
    target : Target | None
        Override default target
    y_label : str | None
        Override default Y-axis label
    include_table : bool
        Include data table below chart
    """

    chart_type: str | None = None  # None = auto-match by method name
    enabled: bool = True
    target: Target | None = None
    y_label: str | None = None
    include_table: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> MethodChartConfig:
        """Create from dictionary."""
        target = None
        if "target" in data:
            t = data["target"]
            target = Target(
                value=t["value"],
                label=t.get("label", "Target"),
                color=t.get("color", "green"),
            )

        return cls(
            chart_type=data.get("chart_type"),  # None if not specified
            enabled=data.get("enabled", True),
            target=target,
            y_label=data.get("y_label"),
            include_table=data.get("include_table", False),
        )


@dataclass
class ChartDefaults:
    """Default chart settings (code defaults, overridable in YAML).

    Attributes
    ----------
    enabled : bool
        Whether charts are enabled by default
    figsize : tuple[float, float]
        Default figure size
    dpi : int
        Default resolution
    """

    enabled: bool = True
    figsize: tuple[float, float] = (10, 6)
    dpi: int = 150

    @classmethod
    def from_dict(cls, data: dict) -> ChartDefaults:
        """Create from dictionary, merging with code defaults."""
        return cls(
            enabled=data.get("enabled", _CODE_DEFAULTS["enabled"]),
            figsize=tuple(data.get("figsize", _CODE_DEFAULTS["figsize"])),
            dpi=data.get("dpi", _CODE_DEFAULTS["dpi"]),
        )


@dataclass
class ChartConfig:
    """Complete chart configuration.

    Can be loaded from YAML or constructed programmatically.
    Code defaults are used unless overridden in YAML.

    Attributes
    ----------
    defaults : ChartDefaults
        Default settings for all charts
    methods : dict[str, MethodChartConfig]
        Per-method chart configurations
    """

    defaults: ChartDefaults = field(default_factory=ChartDefaults)
    methods: dict[str, MethodChartConfig] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> ChartConfig:
        """Load from dictionary (parsed YAML).

        Handles both top-level chart_config and metric-bundled charts.
        """
        defaults = ChartDefaults.from_dict(data.get("defaults", {}))

        methods = {}
        for method_name, method_data in data.get("methods", {}).items():
            methods[method_name] = MethodChartConfig.from_dict(method_data)

        return cls(defaults=defaults, methods=methods)

    @classmethod
    def from_metric_instructions(
        cls, metric_instructions: dict, defaults_data: dict | None = None
    ) -> ChartConfig:
        """Extract chart config from metric_instructions.

        Collects charts from each metric definition.

        Parameters
        ----------
        metric_instructions : dict
            The metric_instructions section from YAML
        defaults_data : dict | None
            Optional defaults override

        Returns
        -------
        ChartConfig
            Merged chart configuration
        """
        defaults = ChartDefaults.from_dict(defaults_data or {})
        methods: dict[str, MethodChartConfig] = {}

        for metric_def in metric_instructions.values():
            charts = metric_def.get("charts", {})
            for method_name, chart_data in charts.items():
                if isinstance(chart_data, dict):
                    methods[method_name] = MethodChartConfig.from_dict(chart_data)
                # If chart_data is just a reference (from anchor), it's already a dict

        return cls(defaults=defaults, methods=methods)

    def get_config_for_method(self, method_name: str) -> MethodChartConfig | None:
        """Get chart config for a method, or None if not configured."""
        return self.methods.get(method_name)

    def add_method(self, method_name: str, config: MethodChartConfig) -> None:
        """Add or update a method's chart configuration."""
        self.methods[method_name] = config


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
