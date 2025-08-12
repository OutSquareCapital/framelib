from dataclasses import dataclass
from typing import Self

import plotly.express as px
import polars as pl

from ._plots import (
    GraphArgs,
    Palette,
    PlotlyTemplate,
    create_plot_function,
    generate_palette,
)
from ._types import ColorMap, Turbo


@dataclass(slots=True)
class Style:
    template: PlotlyTemplate
    color: str
    color_discrete_map: ColorMap

    @classmethod
    def from_df(
        cls,
        df: pl.DataFrame,
        group: str,
        base_palette: Palette = Turbo,
        template: PlotlyTemplate = "plotly_dark",
    ) -> Self:
        keys = df.get_column(group).to_list()
        n_colors: int = len(keys)
        colors: Palette = generate_palette(n_colors, *base_palette)

        return cls(
            color=group,
            color_discrete_map=dict(zip(keys, colors)),
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
