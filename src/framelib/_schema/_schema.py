from __future__ import annotations

from typing import overload

import narwhals as nw
import polars as pl
import pychain as pc
from narwhals.typing import IntoLazyFrameT, LazyFrameT

from .._core import BaseLayout, EntryType
from ._base import Column


class Schema(BaseLayout[Column]):
    """
    A schema is a layout containing only Column entries.
    Used to define the schema of a Table or a File.
    """

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
        - Selects only the columns defined in the schema
        - Casts them to the correct dtype
        - Returns the results wrapped in a narwhals LazyFrame.

        Mostly useful when casting duckdb relations, as this keeps the polars/narwhals API consistent.
        """
        return (
            nw.from_native(df)
            .lazy()
            .select(
                cls.columns().map(lambda col: col.nw_col.cast(col.nw_dtype)).unwrap()
            )
        )

    @overload
    @classmethod
    def cast_native(cls, df: LazyFrameT) -> LazyFrameT: ...

    @overload
    @classmethod
    def cast_native(cls, df: IntoLazyFrameT) -> IntoLazyFrameT: ...

    @overload
    @classmethod
    def cast_native(cls, df: pl.DataFrame) -> pl.LazyFrame: ...

    @classmethod
    def cast_native(
        cls, df: LazyFrameT | pl.DataFrame | IntoLazyFrameT
    ) -> IntoLazyFrameT | LazyFrameT | pl.LazyFrame:
        """
        - Selects only the columns defined in the schema
        - Casts them to the correct dtype
        - Returns the results with the same native DataFrame type as the input.
        """
        return (
            nw.from_native(df)
            .lazy()
            .select(
                cls.columns().map(lambda col: col.nw_col.cast(col.nw_dtype)).unwrap()
            )
            .to_native()
        )
