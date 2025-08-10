from enum import StrEnum, auto
from typing import Literal

type HexColor = str
type Palette = list[HexColor]
type ColorMap = dict[str | int, HexColor]
Formatting = Literal["upper", "lower", "title"]

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


class Tree(StrEnum):
    NODE = "├── "
    LAST_NODE = "└── "
    BRANCH = "│   "
    SPACE = "    "


class CSS(StrEnum):
    DIR = auto()
    FILE = auto()
    STYLE = """
        <style>
            .tree ul { list-style-type: none; padding-left: 20px; }
            .tree li { position: relative; padding-left: 25px; line-height: 1.8; }
            .tree li::before, .tree li::after {
                content: ''; position: absolute; left: 0;
            }
            .tree li::before {
                border-left: 1px solid #999; height: 100%; top: 0; width: 0;
            }
            .tree li:last-child::before { height: 20px; }
            .tree li::after {
                border-top: 1px solid #999; height: 0; top: 20px; width: 18px;
            }
            .tree li.file::after {
                background-image: url('data:image/svg+xml,...'); /* Icône fichier SVG (optionnel) */
            }
            .tree li.dir::after {
                background-image: url('data:image/svg+xml,...'); /* Icône dossier SVG (optionnel) */
            }
        </style>
        """
