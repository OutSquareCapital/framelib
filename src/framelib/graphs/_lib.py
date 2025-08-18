from dataclasses import dataclass
from types import ModuleType
from typing import Self

import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from ._plots import (
    ColorMap,
    GraphArgs,
    Palette,
    PlotlyTemplate,
    create_plot_function,
    generate_palette,
)


def extract_color_scales(module: ModuleType) -> dict[str, list[str]]:
    return {
        key: value
        for key, value in module.__dict__.items()
        if isinstance(value, list) and not key.startswith("_")
    }


@dataclass(slots=True)
class Style:
    template: PlotlyTemplate
    color: str
    color_discrete_map: ColorMap

    @classmethod
    def from_df(cls, df: pl.LazyFrame, col: str) -> Self:
        keys: pl.Series = (
            df.select(pl.col(col)).unique().sort(pl.col(col)).collect().get_column(col)
        )
        return cls.from_series(keys)

    @classmethod
    def from_series(
        cls,
        serie: pl.Series,
        base_palette: Palette = px.colors.sequential.Turbo,
        template: PlotlyTemplate = "plotly_dark",
    ) -> Self:
        return cls(
            color=serie.name,
            color_discrete_map=dict(
                zip(serie.to_list(), generate_palette(serie.len(), *base_palette))
            ),
            template=template,
        )

    @property
    def _arguments(self) -> GraphArgs:
        return GraphArgs(
            template=self.template,
            color=self.color,
            color_discrete_map=self.color_discrete_map,
        )

    def update_color(self, key: str, value: str) -> Self:
        if key in self.color_discrete_map:
            self.color_discrete_map[key] = value
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

    @property
    def scatter(self):
        return create_plot_function(px.scatter, self._arguments)

    @property
    def scatter_3d(self):
        return create_plot_function(px.scatter_3d, self._arguments)

    @property
    def line(self):
        return create_plot_function(px.line, self._arguments)

    @property
    def histogram(self):
        return create_plot_function(px.histogram, self._arguments)

    @property
    def bar(self):
        return create_plot_function(px.bar, self._arguments)

    @property
    def box(self):
        return create_plot_function(px.box, self._arguments)

    @property
    def violin(self):
        return create_plot_function(px.violin, self._arguments)

    @property
    def treemap(self):
        return create_plot_function(px.treemap, self._arguments)

    @property
    def icicle(self):
        return create_plot_function(px.icicle, self._arguments)

    @property
    def sunburst(self):
        return create_plot_function(px.sunburst, self._arguments)
