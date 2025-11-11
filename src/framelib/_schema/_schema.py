from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import overload

import narwhals as nw
import polars as pl
import pyochain as pc
from narwhals.typing import IntoLazyFrameT, LazyFrameT

from .._core import BaseLayout, EntryType
from ._base import Column


class KWord(StrEnum):
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"


def _cols_name(
    cols: pc.Iter[Column], predicate: Callable[[Column], bool]
) -> pc.Seq[str]:
    return cols.filter(predicate).map(lambda col: col.name).collect()


def _cache_from_cols(cols: pc.Iter[Column]) -> KeysConstraints:
    return KeysConstraints(
        primary=cols.pipe(_cols_name, lambda col: col.primary_key),
        unique=cols.pipe(_cols_name, lambda col: col.unique),
    )


@dataclass(slots=True, frozen=True)
class KeysConstraints:
    """Holds the keys constraints of a schema."""

    primary: pc.Seq[str]
    unique: pc.Seq[str]

    @property
    def all(self) -> pc.Seq[str]:
        return self.primary.iter().chain(self.unique.unwrap()).unique().collect()


def _col_from_base(
    base: type[Schema], final_schema: dict[str, Column]
) -> pc.Dict[str, Column]:
    return (
        pc.Dict.from_object(base)
        .filter_attr(base.__entry_type__, Column)
        .for_each(
            lambda name, obj: final_schema.setdefault(name, obj),
        )
    )


def _schema_from_mro(mro: list[type[Schema]]) -> dict[str, Column]:
    final_schema: dict[str, Column] = {}
    (
        pc.Iter.from_(mro)
        .filter_subclass(Schema, keep_parent=False)
        .reverse()
        .for_each(_col_from_base, final_schema)
    )
    return final_schema


class Schema(BaseLayout[Column]):
    """
    A schema is a layout containing only Column entries.
    Used to define the schema of a Table or a File.
    """

    __entry_type__ = EntryType.COLUMN
    _cache: KeysConstraints

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls._cache = cls.schema().iter_values().pipe(_cache_from_cols)
        cls._schema = _schema_from_mro(cls.mro())
        return

    @classmethod
    def constraints(cls) -> KeysConstraints:
        """Returns the keys constraints of the schema."""
        return cls._cache

    @classmethod
    def sql_schema(cls) -> str:
        """
        Generates the SQL schema definition.
        """

        def _convert_col(col: Column) -> str:
            definition: str = f'"{col.name}" {col.sql_type}'
            if col.primary_key:
                definition += " "
                definition += KWord.PRIMARY_KEY
            if col.unique:
                definition += " "
                definition += KWord.UNIQUE
            return definition

        return cls.schema().iter_values().map(_convert_col).into(", ".join)

    @classmethod
    def pl_schema(cls) -> pl.Schema:
        """Returns the Polars schema defined by this Schema."""
        return cls.schema().map_values(lambda c: c.pl_dtype).into(pl.Schema)

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
        """
        return (
            nw.from_native(df)
            .lazy()
            .select(
                cls.schema()
                .iter_values()
                .map(lambda col: col.nw_col.cast(col.nw_dtype))
                .unwrap()
            )
        )

    @classmethod
    def cast_strict_false(cls, df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        """Like `cast`, but with `strict=False`.

        **Warning**

        will only work with polars {Data, Lazy}Frames.
        """
        return df.lazy().select(
            cls.schema()
            .iter_values()
            .map(lambda c: c.pl_col.cast(c.pl_dtype, strict=False))
            .unwrap()
        )
