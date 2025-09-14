from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Concatenate, Self

import marimo as mo
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from ._colors import palette_from_df
from ._types import (
    DataFrameCompatible,
    FigureFunc,
    GraphArgs,
    Templates,
)


@dataclass(slots=True)
class Displayer:
    data_frame: DataFrameCompatible
    template: Templates
    color: str
    color_discrete_map: dict[str, str]
    x: str | None = None
    y: str | None = None
    graphs: list[go.Figure] = field(default_factory=list[go.Figure])

    def pipe[**P, R](
        self, func: Callable[Concatenate[Self, P], R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """
        Pass the Displayer instance and additional arguments to a function and return the result.
        """
        return func(self, *args, **kwargs)

    def _plot_fn[**P](
        self,
        func: FigureFunc[P],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> go.Figure:
        """
        Execute the provided plotting function using the stored DataFrame and default plotting arguments.
        """
        merged = {**self._kw_args, **kwargs}
        return func(self.data_frame, *args, **merged)  # type: ignore

    @property
    def _kw_args(self) -> GraphArgs:
        """
        Returns the default plotting GraphArgs constructed from the Displayer attributes.
        """
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
        df: pl.LazyFrame | pl.DataFrame,
        group: str,
        x: str | None = None,
        y: str | None = None,
        base_palette: list[str] = px.colors.sequential.Turbo,
        template: Templates = "plotly_dark",
    ) -> Self:
        """
        Construct a Displayer from a Polars LazyFrame by collecting and building a color map.
        """
        return cls(
            data_frame=df.lazy().collect(),
            x=x,
            y=y,
            color=group,
            color_discrete_map=df.pipe(palette_from_df, group, base_palette),
            template=template,
        )

    def add_graph[**P](
        self,
        func: FigureFunc[P],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Self:
        """
        Append a generated Plotly figure to the internal graphs list and return self for chaining.
        """
        self.graphs.append(self._plot_fn(func, *args, **kwargs))
        return self

    def plot[**P](
        self,
        func: FigureFunc[P],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> go.Figure:
        """
        Build and return a Plotly figure using the provided function and stored data.
        """
        return self._plot_fn(func, *args, **kwargs)

    def mo_display(self, title: str) -> mo.Html:
        return mo.vstack([mo.md(title), *self.graphs])

    def update_color(self, key: str, value: str) -> Self:
        """
        Update a color in the discrete color map if the key exists and return self.
        """
        if key in self.color_discrete_map:
            self.color_discrete_map[key] = value
        return self

    def set_x(self, x: str | None) -> Self:
        """
        Set the x attribute and return self.
        """
        self.x = x
        return self

    def set_y(self, y: str | None) -> Self:
        """
        Set the y attribute and return self.
        """
        self.y = y
        return self

    def set_data(self, data: DataFrameCompatible) -> Self:
        """
        Replace the stored data_frame and return self.
        """
        self.data_frame = data
        return self

    def set_template(self, template: Templates) -> Self:
        """
        Set the template for the graphs and return self.
        """
        self.template = template
        return self

    def clear(self) -> Self:
        self.graphs.clear()
        return self
