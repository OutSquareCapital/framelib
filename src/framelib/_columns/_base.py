from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal

import narwhals as nw
import polars as pl

from .._core import BaseEntry

TimeUnit = Literal["ns", "us", "ms"]


@dataclass(slots=True, eq=False)
class Column(BaseEntry, ABC):
    """A Column represents a single column in a schema.

    This is the most basic building block of framelib.
    """

    primary_key: bool = field(default=False, kw_only=True)
    """Whether this column is part of the primary key.."""
    unique: bool = field(default=False, kw_only=True)
    """Whether this column has a unique constraint."""
    nullable: bool = field(default=True, kw_only=True)
    """Whether this column can contain null values."""

    @property
    def nw_col(self) -> nw.Expr:
        """Equivalent to `narwhals.col(self._name)`.

        Returns:
            nw.Expr: The `Narwhals` column expression corresponding to this column.
        """
        return nw.col(self._name)

    @property
    def pl_col(self) -> pl.Expr:
        """Equivalent to `polars.col(self._name)`.

        Returns:
            pl.Expr: The `Polars` column expression corresponding to this column.
        """
        return pl.col(self._name)

    @property
    def sql_col(self) -> str:
        """Get the SQL column with name and type.

        Returns:
            str: The SQL column definition.
        """
        return f'"{self._name}" {self.sql_type}'

    @property
    @abstractmethod
    def nw_dtype(self) -> nw.dtypes.DType:
        """Get the Narwhals dtype corresponding to this column."""
        raise NotImplementedError

    @property
    @abstractmethod
    def pl_dtype(self) -> pl.DataType:
        """Get the Polars dtype corresponding to this column."""
        raise NotImplementedError

    @property
    @abstractmethod
    def sql_type(self) -> str:
        """Get the SQL type corresponding to this column."""
        raise NotImplementedError
