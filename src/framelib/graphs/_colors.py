import math
from collections.abc import Iterable
from dataclasses import dataclass
from types import ModuleType
from typing import Self

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import pychain as pc


@dataclass(slots=True)
class RGBColor:
    r: int
    g: int
    b: int

    @classmethod
    def from_hex(cls, hex_color: str) -> Self:
        hex_color = hex_color.lstrip("#")
        result = (
            pc.Iter.from_elements(0, 2, 4)
            .map(lambda i: int(hex_color[i : i + 2], 16))
            .pipe_into(tuple)
        )
        return cls(*result)

    def to_hex(self) -> str:
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    def interpolate(self, other: Self, factor: float) -> Self:
        return self.__class__(
            r=_join_rgb(self.r, other.r, factor),
            g=_join_rgb(self.g, other.g, factor),
            b=_join_rgb(self.b, other.b, factor),
        )


def _join_rgb(left: int, right: int, factor: float) -> int:
    return math.floor(left + (right - left) * factor)


def _get_keys(df: pl.LazyFrame | pl.DataFrame, group: str) -> pc.Iter[str]:
    return pc.Iter(
        df.lazy()
        .select(pl.col(group).unique().sort())
        .collect()
        .get_column(group)
        .to_list()
    )


def palette_from_df(
    df: pl.DataFrame | pl.LazyFrame, group: str, base_palette: Iterable[str]
) -> dict[str, str]:
    return df.pipe(_get_keys, group).pipe(generate_palette, base_palette)


def generate_palette(keys: pc.Iter[str], base_palette: Iterable[str]) -> dict[str, str]:
    n_colors = keys.length()
    base: pc.Iter[str] = pc.Iter(base_palette)
    segments: int = base.length() - 1

    if segments < 1:
        return keys.zip(
            base.head(1).repeat(n_colors).flatten().pipe_into(list)
        ).pipe_into(dict)

    total_interval: int = (n_colors - 1) if n_colors > 1 else 1

    def _calculate_color(i: int) -> str:
        pos: float = _position(i, total_interval, segments)
        index: int = math.floor(pos)
        color_end = RGBColor.from_hex(base.item(min(index + 1, segments)))
        return (
            RGBColor.from_hex(base.item(index))
            .interpolate(color_end, pos - index)
            .to_hex()
        )

    return keys.zip(
        pc.Iter.from_range(0, n_colors).map(_calculate_color).pipe_into(list)
    ).pipe_into(dict)


def _position(i: int, total_interval: int, segments: int) -> float:
    return (i / total_interval) * segments if total_interval > 0 else 0.0


def extract_color_scales(module: ModuleType) -> dict[str, list[str]]:
    return (
        pc.Dict(module.__dict__)
        .filter_items(
            lambda item: isinstance(item[1], list) and not item[0].startswith("_")
        )
        .unwrap()
    )


def combine_palettes(*palettes: Iterable[str]) -> list[str]:
    return pc.Iter.from_elements(*palettes).flatten().pipe_into(list)


def show_colors_scale() -> go.Figure:
    """
    Return a Plotly figure showing the color swatches.
    """
    return px.colors.sequential.swatches().update_layout(
        title=None,
        height=550,
        width=400,
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        paper_bgcolor="#181c1a",
    )
