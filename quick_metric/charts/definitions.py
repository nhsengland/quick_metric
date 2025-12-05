"""
Base chart type definitions.

Provides abstract base classes for chart types that can be extended
by consuming pipelines for domain-specific charts.

Example Usage in Consuming Pipeline
-----------------------------------
```python
from quick_metric.charts import LineChart, ColumnChart, Target

class ComplianceRateChart(LineChart):
    '''Line chart for compliance rate methods.'''

    y_label = "Compliance Rate (%)"
    default_target = Target(value=0.95, label="95% Target")

    def matches(self, method_name: str) -> bool:
        return "compliance_rate" in method_name.lower()
```
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from quick_metric.charts.core import ChartSettings, Target, snake_to_title

if TYPE_CHECKING:
    pass


class BaseChart(ABC):
    """Abstract base class for chart types.

    Subclass this to create domain-specific chart types in your pipeline.
    The chart type determines rendering style and default settings.

    Attributes
    ----------
    chart_type : str
        Rendering type: 'line', 'column', or 'bar'
    y_label : str
        Default Y-axis label
    default_target : Target | None
        Default target line (can be overridden per-call)
    """

    chart_type: str = "line"
    y_label: str = "Value"
    default_target: Target | None = None

    @abstractmethod
    def matches(self, method_name: str) -> bool:
        """Check if this chart type should be used for a method.

        Parameters
        ----------
        method_name : str
            Name of the metric method

        Returns
        -------
        bool
            True if this chart type matches the method
        """

    def get_title(self, method_name: str, metric_name: str | None = None) -> str:
        """Generate chart title from method and metric names.

        Parameters
        ----------
        method_name : str
            Name of the metric method
        metric_name : str | None
            Name of the parent metric

        Returns
        -------
        str
            Chart title
        """
        title = snake_to_title(method_name)
        if metric_name:
            title = f"{snake_to_title(metric_name)}: {title}"
        return title

    def get_settings(self, **overrides: object) -> ChartSettings:
        """Get chart settings with defaults and any overrides.

        Parameters
        ----------
        **overrides
            Settings to override defaults

        Returns
        -------
        ChartSettings
            Merged settings
        """
        settings = ChartSettings(
            y_label=self.y_label,
            target=self.default_target,
        )

        # Apply overrides
        for key, value in overrides.items():
            if hasattr(settings, key) and value is not None:
                setattr(settings, key, value)

        return settings


class LineChart(BaseChart):
    """Base line chart type.

    Use for time series, trends, and continuous data.
    """

    chart_type = "line"

    def matches(self, method_name: str) -> bool:
        """Default: match rate/percentage methods."""
        method_lower = method_name.lower()
        return any(term in method_lower for term in ["rate", "percentage", "percent", "trend"])


class ColumnChart(BaseChart):
    """Base column (vertical bar) chart type.

    Use for counts, volumes, and categorical comparisons.
    """

    chart_type = "column"
    y_label = "Count"

    def matches(self, method_name: str) -> bool:
        """Default: match count/volume methods."""
        method_lower = method_name.lower()
        return any(term in method_lower for term in ["count", "volume", "total"])


class BarChart(BaseChart):
    """Base bar (horizontal) chart type.

    Use for categorical comparisons where labels are long.
    """

    chart_type = "bar"
    y_label = "Value"

    def matches(self, method_name: str) -> bool:
        """Default: match breakdown/by_ methods."""
        method_lower = method_name.lower()
        return any(term in method_lower for term in ["breakdown", "by_"])
