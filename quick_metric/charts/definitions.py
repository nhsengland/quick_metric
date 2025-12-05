"""
Base chart class and common chart type definitions.

Provides the abstract base class for chart types and common
NHS-themed chart definitions for compliance metrics.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from quick_metric.charts.core import register_chart, snake_to_title


class BaseChart(ABC):
    """Abstract base class for chart types.

    Chart types define how to match methods and generate chart metadata.
    They don't render charts directly - that's handled by renderers.

    Attributes
    ----------
    chart_type : str
        Type of chart (e.g., 'line', 'column', 'bar')
    display_name : str
        Human-readable name for the chart type
    """

    chart_type: str = "line"
    display_name: str = "Chart"
    _enabled: bool = True

    @abstractmethod
    def matches(self, method_name: str) -> bool:
        """Check if this chart type matches a method name.

        Parameters
        ----------
        method_name : str
            Name of the metric method

        Returns
        -------
        bool
            True if this chart type should be used for the method
        """

    def get_title(self, method_name: str) -> str:
        """Generate chart title from method name.

        Parameters
        ----------
        method_name : str
            Name of the metric method

        Returns
        -------
        str
            Chart title
        """
        return snake_to_title(method_name)

    def get_y_axis_name(self, method_name: str) -> str:
        """Get Y-axis label for the chart.

        Parameters
        ----------
        method_name : str
            Name of the metric method

        Returns
        -------
        str
            Y-axis label
        """
        if self.is_percentage(method_name):
            return "Percentage (%)"
        return "Value"

    def get_x_axis_name(self) -> str:
        """Get X-axis label for the chart.

        Returns
        -------
        str
            X-axis label
        """
        return "Period"

    def is_percentage(self, method_name: str) -> bool:
        """Check if the method produces percentage values.

        Parameters
        ----------
        method_name : str
            Name of the metric method

        Returns
        -------
        bool
            True if values should be displayed as percentages
        """
        method_lower = method_name.lower()
        return any(term in method_lower for term in ["rate", "percentage", "percent", "compliance"])


# =============================================================================
# Common Chart Types
# =============================================================================


@register_chart(enabled=True)
class ComplianceRateChart(BaseChart):
    """Line chart for compliance rates and percentages.

    Matches methods containing 'rate', 'percentage', or 'compliance'.
    Displays values as percentages with target lines.
    """

    chart_type = "line"
    display_name = "Compliance Rate"

    def matches(self, method_name: str) -> bool:
        """Match rate, percentage, or compliance methods."""
        method_lower = method_name.lower()
        return any(term in method_lower for term in ["rate", "percentage", "compliance", "percent"])

    def get_y_axis_name(self, method_name: str) -> str:
        """Return percentage label."""
        return "Compliance Rate (%)"


@register_chart(enabled=True)
class ComplianceCountChart(BaseChart):
    """Column chart for counts and volumes.

    Matches methods containing 'count' or 'volume'.
    """

    chart_type = "column"
    display_name = "Compliance Count"

    def matches(self, method_name: str) -> bool:
        """Match count or volume methods."""
        method_lower = method_name.lower()
        return any(term in method_lower for term in ["count", "volume", "total"])

    def get_y_axis_name(self, method_name: str) -> str:
        """Return count label."""
        return "Count"

    def is_percentage(self, method_name: str) -> bool:
        """Counts are never percentages."""
        return False


@register_chart(enabled=True)
class MeanDaysChart(BaseChart):
    """Line chart for mean days metrics.

    Matches methods containing both 'mean' and 'days'.
    """

    chart_type = "line"
    display_name = "Mean Days"

    def matches(self, method_name: str) -> bool:
        """Match methods with mean and days."""
        method_lower = method_name.lower()
        return "mean" in method_lower and "days" in method_lower

    def get_y_axis_name(self, method_name: str) -> str:
        """Return days label."""
        return "Mean Days"

    def is_percentage(self, method_name: str) -> bool:
        """Days are never percentages."""
        return False


@register_chart(enabled=True)
class BacklogChart(BaseChart):
    """Column chart for backlog metrics.

    Matches methods containing 'backlog'.
    """

    chart_type = "column"
    display_name = "Backlog"

    def matches(self, method_name: str) -> bool:
        """Match backlog methods."""
        return "backlog" in method_name.lower()

    def get_y_axis_name(self, method_name: str) -> str:
        """Return backlog label."""
        return "Backlog Count"

    def is_percentage(self, method_name: str) -> bool:
        """Backlog counts are never percentages."""
        return False
