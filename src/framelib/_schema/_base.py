from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Final, Literal

import narwhals as nw
import polars as pl

from .._core import BaseEntry

TimeUnit = Literal["ns", "us", "ms"]


@dataclass(slots=True)
class Column(BaseEntry, ABC):
    _is_column: Final[bool] = field(init=False, default=True)
    primary_key: bool = False
    unique: bool = False

    @property
    def nw_col(self) -> nw.Expr:
        """Return `narwhals.col(self._name)`"""
        return nw.col(self._name)

    @property
    def pl_col(self) -> pl.Expr:
        """Return `polars.col(self._name)`"""
        return pl.col(self._name)

    @property
    @abstractmethod
    def nw_dtype(self) -> nw.dtypes.DType:
        """The Narwhals dtype corresponding to this column."""
        raise NotImplementedError

    @property
    @abstractmethod
    def pl_dtype(self) -> pl.DataType:
        """The Polars dtype corresponding to this column."""
        raise NotImplementedError

    @property
    def sql_type(self) -> str:
        """The SQL type corresponding to this column."""
        raise NotImplementedError
