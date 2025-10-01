from __future__ import annotations

from typing import overload

import narwhals as nw
import polars as pl
import pychain as pc
from narwhals.typing import IntoLazyFrameT, LazyFrameT

from .._core import BaseLayout, EntryType
from ._columns import Column


class Schema(BaseLayout[Column]):
    _is_entry_type = EntryType.COLUMN

    @classmethod
    def columns(cls) -> pc.Dict[str, Column]:
        """
        Returns a dictionary of the Column instances in the folder.
        """
        return pc.Dict(cls._schema)

    @classmethod
    def column_names(cls) -> pc.Iter[str]:
        """The column names of this schema."""
        return pc.Iter(cls._schema.keys())

    @classmethod
    def column_exprs(cls) -> pc.Iter[nw.Expr]:
        """The column expressions of this schema."""
        return pc.Iter(col.col for col in cls._schema.values())

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
        return (
            nw.from_native(df)
            .lazy()
            .select([col.col.cast(col.dtype) for col in cls._schema.values()])
        )
