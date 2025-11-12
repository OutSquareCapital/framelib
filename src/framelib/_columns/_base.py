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
        """
        Equivalent to `narwhals.col(self._name)`.

        Returns:
            out (narwhals.Expr): The `Narwhals` column expression corresponding to this column.
        """
        return nw.col(self._name)

    @property
    def pl_col(self) -> pl.Expr:
        """
        Equivalent to `polars.col(self._name)`.
        Returns:
            out (polars.Expr): The `Polars` column expression corresponding to this column.
        """
        return pl.col(self._name)

    @property
    @abstractmethod
    def nw_dtype(self) -> nw.dtypes.DType:
        """
        Returns:
            out (narwhals.dtypes.DType): The Narwhals dtype corresponding to this column.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def pl_dtype(self) -> pl.DataType:
        """
        Returns:
            out (polars.DataType): The Polars dtype corresponding to this column.
        """
        raise NotImplementedError

    @property
    def sql_type(self) -> str:
        """
        Returns:
            str: The SQL type corresponding to this column.
        """
        raise NotImplementedError
