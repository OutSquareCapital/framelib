from abc import ABC, abstractmethod
from typing import Literal

import narwhals as nw
import polars as pl

from .._core import BaseEntry

TimeUnit = Literal["ns", "us", "ms"]


class Column(BaseEntry, ABC):
    """A Column represents a single column in a schema.

    This is the most basic building block of framelib.

    Args:
        primary_key (bool): Whether this column is part of the primary key.
        unique (bool): Whether this column has a unique constraint.
        nullable (bool): Whether this column can contain null values.
    """

    __slots__ = ("_nullable", "_primary_key", "_unique")

    def __init__(
        self,
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
    ) -> None:
        self._primary_key: bool = primary_key
        self._unique: bool = unique
        self._nullable: bool = nullable

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

    @property
    def primary_key(self) -> bool:
        """Check if this column is part of the primary key.

        Returns:
            bool: True if this column is part of the primary key, False otherwise.
        """
        return self._primary_key

    @property
    def unique(self) -> bool:
        """Check if this column has a unique constraint.

        Returns:
            bool: True if this column has a unique constraint, False otherwise.
        """
        return self._unique

    @property
    def nullable(self) -> bool:
        """Check if this column can contain NULL values.

        Returns:
            bool: True if this column is nullable, False otherwise.
        """
        return self._nullable
