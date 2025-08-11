from typing import Literal

type HexColor = str
type Palette = list[HexColor]
type ColorMap = dict[str | int, HexColor]


PlotlyTemplate = Literal[
    "ggplot2",
    "seaborn",
    "simple_white",
    "plotly",
    "plotly_white",
    "plotly_dark",
    "presentation",
    "xgridoff",
    "ygridoff",
    "gridon",
    "none",
]


Turbo: Palette = [
    "#383fff",
    "#1d53dc",
    "#32a0ff",
    "#1bcfd4",
    "#24eca6",
    "#61fc6c",
    "#a4fc3b",
    "#d1e834",
    "#f3c63a",
    "#fe9b2d",
    "#f36315",
    "#d93806",
    "#b11901",
    "#7a0402",
]
