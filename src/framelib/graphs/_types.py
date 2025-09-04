from collections.abc import Callable, Sequence
from typing import Any, Concatenate, Literal, Protocol, TypedDict, get_args

import plotly.graph_objects as go
import polars as pl


class DataFrameCompatible(Protocol):
    # More details at https://data-apis.org/dataframe-protocol/latest/index.html
    def __dataframe__(self, nan_as_null: bool = ..., allow_copy: bool = ...) -> Any: ...


type ArrayLike = Sequence[Any] | pl.Series
type FrameOrDict = DataFrameCompatible | dict[str, ArrayLike] | Sequence[dict[str, Any]]
type ColorMap = dict[str | int, str]
type FigureFunc[**P] = Callable[Concatenate[DataFrameCompatible, P], go.Figure]

Templates = Literal[
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

TemplatesValues: tuple[str, ...] = get_args(Templates)


class GraphArgs(TypedDict):
    x: str | None
    y: str | None
    template: Templates
    color: str
    color_discrete_map: ColorMap
