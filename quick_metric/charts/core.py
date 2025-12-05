"""
Chart configuration and NHS colour palette.

Provides simple chart settings and utilities. Configuration follows DRY -
set defaults once, override only when needed.
"""

from __future__ import annotations

from dataclasses import dataclass

# =============================================================================
# NHS Colour Palette
# =============================================================================

NHS_COLOURS: dict[str, str] = {
    # Primary colours
    "blue": "#005EB8",  # NHS Blue (primary brand colour)
    "dark_blue": "#003087",  # NHS Dark Blue
    "bright_blue": "#0072CE",  # NHS Bright Blue
    "light_blue": "#41B6E6",  # NHS Light Blue
    # Secondary colours
    "green": "#009639",  # NHS Green (positive/success)
    "aqua_green": "#00A499",  # NHS Aqua Green
    "orange": "#ED8B00",  # NHS Orange (warning)
    "yellow": "#FFB81C",  # NHS Warm Yellow
    "dark_red": "#8A1538",  # NHS Dark Red (error/danger)
    "pink": "#AE2573",  # NHS Pink
    "purple": "#330072",  # NHS Purple
    # Neutral colours
    "dark_grey": "#425563",  # NHS Dark Grey
    "mid_grey": "#768692",  # NHS Mid Grey
    "pale_grey": "#E8EDEE",  # NHS Pale Grey
    "white": "#FFFFFF",
    "black": "#231F20",
}

# Colour cycle for multiple series (NHS brand compliant)
NHS_COLOUR_CYCLE: list[str] = [
    NHS_COLOURS["blue"],
    NHS_COLOURS["aqua_green"],
    NHS_COLOURS["orange"],
    NHS_COLOURS["purple"],
    NHS_COLOURS["pink"],
    NHS_COLOURS["dark_blue"],
    NHS_COLOURS["green"],
    NHS_COLOURS["yellow"],
]


# =============================================================================
# Chart Settings
# =============================================================================


@dataclass
class Target:
    """Target line for a chart.

    Attributes
    ----------
    value : float
        Target value (e.g., 0.95 for 95%)
    label : str
        Display label
    color : str
        Colour name from NHS_COLOURS or hex code
    """

    value: float
    label: str = "Target"
    color: str = "green"


@dataclass
class ChartSettings:
    """Settings for chart rendering.

    Simple settings with sensible defaults. Override only what you need.

    Attributes
    ----------
    figsize : tuple[float, float]
        Figure size in inches (width, height)
    dpi : int
        Resolution in dots per inch
    include_table : bool
        Whether to include data table below chart
    target : Target | None
        Optional target line
    title : str | None
        Override auto-generated title
    x_label : str
        X-axis label
    y_label : str | None
        Y-axis label (None for auto-detect)
    footer : str | None
        Footer text for the chart
    """

    figsize: tuple[float, float] = (10, 6)
    dpi: int = 150
    include_table: bool = False
    target: Target | None = None
    title: str | None = None
    x_label: str = "Period"
    y_label: str | None = None
    footer: str | None = None


# =============================================================================
# Utilities
# =============================================================================


def snake_to_title(text: str) -> str:
    """Convert snake_case to Title Case.

    Parameters
    ----------
    text : str
        Snake case text

    Returns
    -------
    str
        Title case text

    Examples
    --------
    >>> snake_to_title("compliance_rate")
    'Compliance Rate'
    """
    return " ".join(word.capitalize() for word in text.replace("_", " ").split())


def get_color(color_name: str) -> str:
    """Get colour hex code from name or return as-is if hex.

    Parameters
    ----------
    color_name : str
        NHS colour name or hex code

    Returns
    -------
    str
        Hex colour code
    """
    return NHS_COLOURS.get(color_name, color_name)
