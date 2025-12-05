"""
Chart configuration, registry, and target line functionality.

Provides the core infrastructure for chart configuration including:
- Dataclasses for chart settings
- Chart type registry with decorator pattern
- Target line configuration for benchmarks
- YAML configuration loading
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger
import yaml

if TYPE_CHECKING:
    from quick_metric.charts.definitions import BaseChart

# =============================================================================
# Dataclasses
# =============================================================================


@dataclass
class ChartDefaults:
    """Default settings for all charts.

    Attributes
    ----------
    enabled : bool
        Whether charts are enabled by default
    include_table : bool
        Whether to include data table below chart
    figsize : tuple[float, float]
        Figure size in inches (width, height)
    dpi : int
        Resolution in dots per inch
    """

    enabled: bool = True
    include_table: bool = False
    figsize: tuple[float, float] = (10, 6)
    dpi: int = 150


@dataclass
class ChartTarget:
    """Target line configuration for a chart.

    Attributes
    ----------
    value : float
        Target value (e.g., 0.95 for 95% compliance)
    label : str
        Display label for the target line
    color : str
        Color name from NHS palette or hex code
    """

    value: float
    label: str = "Target"
    color: str = "green"


@dataclass
class MethodChartConfig:
    """Chart configuration for a specific method.

    Attributes
    ----------
    enabled : bool | None
        Override default enabled setting
    chart_type : str | None
        Override default chart type
    include_table : bool | None
        Override default include_table setting
    figsize : tuple[float, float] | None
        Override default figure size
    target : ChartTarget | None
        Target line configuration
    """

    enabled: bool | None = None
    chart_type: str | None = None
    include_table: bool | None = None
    figsize: tuple[float, float] | None = None
    target: ChartTarget | None = None


@dataclass
class ChartConfig:
    """Complete chart configuration.

    Attributes
    ----------
    defaults : ChartDefaults
        Default settings for all charts
    targets : dict[str, ChartTarget]
        Named target configurations
    methods : dict[str, MethodChartConfig]
        Method-specific chart configurations
    """

    defaults: ChartDefaults = field(default_factory=ChartDefaults)
    targets: dict[str, ChartTarget] = field(default_factory=dict)
    methods: dict[str, MethodChartConfig] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> ChartConfig:
        """Create ChartConfig from dictionary (e.g., parsed YAML).

        Parameters
        ----------
        config_dict : dict
            Dictionary containing chart configuration

        Returns
        -------
        ChartConfig
            Parsed configuration object
        """
        defaults_dict = config_dict.get("defaults", {})
        defaults = ChartDefaults(
            enabled=defaults_dict.get("enabled", True),
            include_table=defaults_dict.get("include_table", False),
            figsize=tuple(defaults_dict.get("figsize", [10, 6])),
            dpi=defaults_dict.get("dpi", 150),
        )

        targets = {}
        for name, target_dict in config_dict.get("targets", {}).items():
            targets[name] = ChartTarget(
                value=target_dict["value"],
                label=target_dict.get("label", "Target"),
                color=target_dict.get("color", "green"),
            )

        methods = {}
        for name, method_dict in config_dict.get("methods", {}).items():
            target = None
            if "target" in method_dict:
                target_data = method_dict["target"]
                target = ChartTarget(
                    value=target_data["value"],
                    label=target_data.get("label", "Target"),
                    color=target_data.get("color", "green"),
                )

            figsize = None
            if "figsize" in method_dict:
                figsize = tuple(method_dict["figsize"])

            methods[name] = MethodChartConfig(
                enabled=method_dict.get("enabled"),
                chart_type=method_dict.get("chart_type"),
                include_table=method_dict.get("include_table"),
                figsize=figsize,
                target=target,
            )

        return cls(defaults=defaults, targets=targets, methods=methods)

    def get_effective_settings(self, method_name: str) -> dict[str, Any]:
        """Get effective settings for a method, merging defaults with overrides.

        Parameters
        ----------
        method_name : str
            Name of the method

        Returns
        -------
        dict
            Merged settings with method-specific overrides applied
        """
        settings = {
            "enabled": self.defaults.enabled,
            "include_table": self.defaults.include_table,
            "figsize": self.defaults.figsize,
            "dpi": self.defaults.dpi,
            "target": None,
        }

        # Check if there's a target for this method
        if method_name in self.targets:
            settings["target"] = self.targets[method_name]

        # Apply method-specific overrides
        if method_name in self.methods:
            method_config = self.methods[method_name]
            if method_config.enabled is not None:
                settings["enabled"] = method_config.enabled
            if method_config.include_table is not None:
                settings["include_table"] = method_config.include_table
            if method_config.figsize is not None:
                settings["figsize"] = method_config.figsize
            if method_config.target is not None:
                settings["target"] = method_config.target

        return settings


# =============================================================================
# Chart Registry
# =============================================================================

_CHART_REGISTRY: dict[str, BaseChart] = {}


def register_chart(enabled: bool = True):
    """Decorator to register a chart class.

    Parameters
    ----------
    enabled : bool
        Whether this chart type is enabled by default

    Returns
    -------
    Callable
        Decorator function
    """

    def decorator(cls):
        instance = cls()
        instance._enabled = enabled
        _CHART_REGISTRY[cls.__name__] = instance
        logger.debug(f"Registered chart type: {cls.__name__}")
        return cls

    return decorator


def get_registered_charts() -> dict[str, BaseChart]:
    """Get all registered chart types.

    Returns
    -------
    dict[str, BaseChart]
        Dictionary of chart class name to instance
    """
    return _CHART_REGISTRY.copy()


def get_chart_for_method(method_name: str) -> BaseChart | None:
    """Find the appropriate chart type for a method.

    Parameters
    ----------
    method_name : str
        Name of the metric method

    Returns
    -------
    BaseChart | None
        Matching chart instance or None if no match
    """
    for chart in _CHART_REGISTRY.values():
        if chart.matches(method_name):
            return chart
    logger.debug(f"No chart type registered for method: {method_name}")
    return None


def should_create_chart(method_name: str) -> bool:
    """Check if a chart should be created for a method.

    Parameters
    ----------
    method_name : str
        Name of the metric method

    Returns
    -------
    bool
        True if a chart should be created
    """
    config = get_chart_config()
    settings = config.get_effective_settings(method_name)

    if not settings["enabled"]:
        return False

    chart = get_chart_for_method(method_name)
    return chart is not None


def get_method_chart_settings(method_name: str) -> dict[str, Any]:
    """Get chart settings for a specific method.

    Parameters
    ----------
    method_name : str
        Name of the metric method

    Returns
    -------
    dict
        Chart settings including chart type, figsize, etc.
    """
    config = get_chart_config()
    settings = config.get_effective_settings(method_name)

    chart = get_chart_for_method(method_name)
    if chart:
        settings["chart_type"] = chart.chart_type
        settings["chart_class"] = chart

    return settings


# =============================================================================
# Target Lines
# =============================================================================

CHART_TARGETS: dict[str, dict[str, Any]] = {
    "compliance_rate": {"value": 0.95, "label": "95% Target", "color": "green"},
    "percentage": {"value": 0.95, "label": "95% Target", "color": "green"},
}


def get_target_for_method(method_name: str) -> dict[str, Any] | None:
    """Get target configuration for a method.

    Checks in order:
    1. Global chart config targets
    2. Default CHART_TARGETS patterns

    Parameters
    ----------
    method_name : str
        Name of the metric method

    Returns
    -------
    dict | None
        Target configuration or None
    """
    config = get_chart_config()
    settings = config.get_effective_settings(method_name)

    if settings.get("target"):
        target = settings["target"]
        return {"value": target.value, "label": target.label, "color": target.color}

    # Check default patterns
    method_lower = method_name.lower()
    for pattern, target_config in CHART_TARGETS.items():
        if pattern in method_lower:
            return target_config.copy()

    return None


def set_chart_target(
    method_pattern: str,
    value: float,
    label: str = "Target",
    color: str = "green",
) -> None:
    """Set a chart target for methods matching a pattern.

    Parameters
    ----------
    method_pattern : str
        Pattern to match method names (substring match)
    value : float
        Target value
    label : str
        Display label for target line
    color : str
        Color name from NHS palette
    """
    CHART_TARGETS[method_pattern] = {"value": value, "label": label, "color": color}
    logger.debug(f"Set chart target for pattern '{method_pattern}': {value}")


def clear_chart_target(method_pattern: str) -> None:
    """Remove a chart target pattern.

    Parameters
    ----------
    method_pattern : str
        Pattern to remove
    """
    if method_pattern in CHART_TARGETS:
        del CHART_TARGETS[method_pattern]
        logger.debug(f"Cleared chart target for pattern: {method_pattern}")


# =============================================================================
# Configuration Loading
# =============================================================================

_GLOBAL_CHART_CONFIG: ChartConfig | None = None


def load_chart_config(yaml_path: Path | str) -> ChartConfig:
    """Load chart configuration from a YAML file.

    Parameters
    ----------
    yaml_path : Path | str
        Path to YAML configuration file

    Returns
    -------
    ChartConfig
        Parsed configuration
    """
    path = Path(yaml_path)
    if not path.exists():
        logger.warning(f"Chart config file not found: {path}")
        return ChartConfig()

    with open(path) as f:
        data = yaml.safe_load(f)

    chart_config = data.get("chart_config", {})
    return ChartConfig.from_dict(chart_config)


def get_chart_config() -> ChartConfig:
    """Get the current global chart configuration.

    Returns
    -------
    ChartConfig
        Current configuration or default if not set
    """
    if _GLOBAL_CHART_CONFIG is None:
        return ChartConfig()
    return _GLOBAL_CHART_CONFIG


def set_chart_config(config: ChartConfig) -> None:
    """Set the global chart configuration.

    Parameters
    ----------
    config : ChartConfig
        Configuration to use globally
    """
    global _GLOBAL_CHART_CONFIG  # noqa: PLW0603
    _GLOBAL_CHART_CONFIG = config
    logger.debug("Global chart configuration updated")


# =============================================================================
# Utilities
# =============================================================================


def snake_to_title(text: str) -> str:
    """Convert snake_case to Title Case.

    Parameters
    ----------
    text : str
        Snake case text (e.g., 'monthly_compliance_rate')

    Returns
    -------
    str
        Title case text (e.g., 'Monthly Compliance Rate')
    """
    return " ".join(word.capitalize() for word in text.split("_"))
