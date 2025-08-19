from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Concatenate, Self

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from ._colors import get_color_map
from ._types import ColorMap, DataFrameCompatible, GraphArgs, Palette, Templates


def _get_keys(df: pl.LazyFrame, col: str) -> pl.Series:
    return df.select(pl.col(col)).unique().sort(pl.col(col)).collect().get_column(col)


@dataclass(slots=True)
class Displayer:
    data_frame: DataFrameCompatible
    x: str
    y: str
    template: Templates
    color: str
    color_discrete_map: ColorMap
    graphs: list[go.Figure] = field(default_factory=list[go.Figure])

    def _plot_fn[**P](
        self,
        func: Callable[Concatenate[DataFrameCompatible, P], go.Figure],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> go.Figure:
        return func(self.data_frame, *args, **self._kw_args, **kwargs)

    @property
    def _kw_args(self) -> GraphArgs:
        return GraphArgs(
            x=self.x,
            y=self.y,
            template=self.template,
            color=self.color,
            color_discrete_map=self.color_discrete_map,
        )

    @classmethod
    def from_df(
        cls,
        df: pl.LazyFrame,
        col: str,
        x: str,
        y: str,
        base_palette: Palette = px.colors.sequential.Turbo,
        template: Templates = "plotly_dark",
    ) -> Self:
        discrete_map: ColorMap = get_color_map(_get_keys(df, col), base_palette)
        return cls(
            data_frame=df.collect(),
            x=x,
            y=y,
            color=col,
            color_discrete_map=discrete_map,
            template=template,
        )

    def add_graph[**P](
        self,
        func: Callable[Concatenate[DataFrameCompatible, P], go.Figure],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Self:
        self.graphs.append(self._plot_fn(func, *args, **kwargs))
        return self

    def plot[**P](
        self,
        func: Callable[Concatenate[DataFrameCompatible, P], go.Figure],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> go.Figure:
        return self._plot_fn(func, *args, **kwargs)

    def update_color(self, key: str, value: str) -> Self:
        if key in self.color_discrete_map:
            self.color_discrete_map[key] = value
        return self

    def set_x(self, x: str) -> Self:
        self.x = x
        return self

    def set_y(self, y: str) -> Self:
        self.y = y
        return self

    def set_data(self, data: DataFrameCompatible) -> Self:
        self.data_frame = data
        return self

    def show_colors_scale(self) -> go.Figure:
        return px.colors.sequential.swatches().update_layout(
            template=self.template,
            title=None,
            height=550,
            width=400,
            margin={"l": 0, "r": 0, "t": 0, "b": 0},
            paper_bgcolor="#181c1a",
        )
