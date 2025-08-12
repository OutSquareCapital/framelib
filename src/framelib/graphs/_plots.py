import math
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from typing import Literal, Self, TypedDict

import plotly.graph_objects as go

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


class GraphArgs(TypedDict):
    template: PlotlyTemplate
    color: str
    color_discrete_map: ColorMap


@dataclass(slots=True)
class RGBColor:
    r: int
    g: int
    b: int

    @classmethod
    def from_hex(cls, hex_color: HexColor) -> Self:
        hex_color = hex_color.lstrip("#")
        result = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
        return cls(*result)

    def to_hex(self) -> HexColor:
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    def interpolate(self, other: Self, factor: float) -> "RGBColor":
        return RGBColor(
            r=math.floor(self.r + (other.r - self.r) * factor),
            g=math.floor(self.g + (other.g - self.g) * factor),
            b=math.floor(self.b + (other.b - self.b) * factor),
        )


def generate_palette(n_colors: int, *base_palette: HexColor) -> Palette:
    palette_len = len(base_palette)

    segments: int = palette_len - 1

    if segments < 1:
        return [base_palette[0]] * n_colors

    result: list[HexColor] = []
    total_interval: int = (n_colors - 1) if n_colors > 1 else 1
    for i in range(n_colors):
        pos: float = (i / total_interval) * segments if total_interval > 0 else 0.0
        index: int = math.floor(pos)
        factor: float = pos - index

        c2_hex = base_palette[min(index + 1, segments)]

        result.append(
            RGBColor.from_hex(base_palette[index])
            .interpolate(RGBColor.from_hex(c2_hex), factor)
            .to_hex()
        )

    return result


def create_plot_function[**P](
    func: Callable[P, go.Figure], arguments: GraphArgs
) -> Callable[P, go.Figure]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> go.Figure:
        merged_kwargs = {**arguments, **kwargs}
        return func(*args, **merged_kwargs)  # type: ignore[return-value]

    return wrapper
