from __future__ import annotations

from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Literal

import narwhals as nw
import polars as pl

from .._core import BaseEntry


class _KWord(StrEnum):
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"


TimeUnit = Literal["ns", "us", "ms"]


class Column(BaseEntry, ABC):
    """A Column represents a single column in a schema.

    This is the most basic building block of framelib.

    Args:
        primary_key (bool): Whether this column is part of the primary key.
        unique (bool): Whether this column has a unique constraint.
    """

    def __init__(
        self,
        *,
        primary_key: bool = False,
        unique: bool = False,
    ) -> None:
        self._primary_key: bool = primary_key
        self._unique: bool = unique

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

    def to_sql(self) -> str:
        """Generates the SQL representation of the column.

        Additional constraints such as PRIMARY KEY and UNIQUE are included.
        """
        definition: str = f'"{self.name}" {self.sql_type}'
        if self.primary_key:
            definition += " "
            definition += _KWord.PRIMARY_KEY
        if self.unique:
            definition += " "
            definition += _KWord.UNIQUE
        return definition

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
