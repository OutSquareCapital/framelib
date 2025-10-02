from __future__ import annotations

from typing import overload

import narwhals as nw
import polars as pl
import pychain as pc
from narwhals.typing import IntoLazyFrameT, LazyFrameT

from ._columns import Column
from ._core import BaseLayout, EntryType


class Schema(BaseLayout[Column]):
    _is_entry_type = EntryType.COLUMN

    @classmethod
    def columns(cls) -> pc.Iter[Column]:
        """
        Returns an iterator over the Column instances in the folder.
        """
        return pc.Iter(cls._schema.values())

    @classmethod
    def column_names(cls) -> pc.Iter[str]:
        """The column names of this schema."""
        return pc.Iter(cls._schema.keys())

    @classmethod
    def column_exprs(cls) -> pc.Iter[nw.Expr]:
        """The column expressions of this schema."""
        return pc.Iter(col.col for col in cls._schema.values())

    @classmethod
    def primary_keys(cls) -> pc.Iter[str]:
        """The primary key columns of this schema."""
        return (
            cls.columns().filter(lambda col: col.primary_key).map(lambda col: col.name)
        )

    @overload
    @classmethod
    def cast(cls, df: LazyFrameT) -> LazyFrameT: ...

    @overload
    @classmethod
    def cast(cls, df: IntoLazyFrameT) -> nw.LazyFrame[IntoLazyFrameT]: ...

    @overload
    @classmethod
    def cast(cls, df: pl.DataFrame) -> nw.LazyFrame[pl.LazyFrame]: ...

    @classmethod
    def cast(
        cls,
        df: IntoLazyFrameT | LazyFrameT | pl.DataFrame,
    ) -> LazyFrameT | nw.LazyFrame[IntoLazyFrameT]:
        """
        Selects only the columns defined in the schema, and casts them to the correct dtype.
        """
        return (
            nw.from_native(df)
            .lazy()
            .select(cls.columns().map(lambda col: col.col.cast(col.dtype)).unwrap())
        )
