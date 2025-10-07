from __future__ import annotations

from enum import StrEnum
from typing import Self, overload

import narwhals as nw
import polars as pl
import pychain as pc
from narwhals.typing import IntoLazyFrameT, LazyFrameT

from .._core import BaseLayout, EntryType
from ._base import Column


class KWord(StrEnum):
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"


class Schema(BaseLayout[Column]):
    """
    A schema is a layout containing only Column entries.
    Used to define the schema of a Table or a File.
    """

    _entry_type = EntryType.COLUMN
    _primary_keys: pc.Iter[str]
    _unique_keys: pc.Iter[str]
    _constraint_keys: pc.Iter[str]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls._set_keys()._set_schema()
        return

    @classmethod
    def _set_keys(cls) -> type[Self]:
        cls._primary_keys = (
            cls.columns()
            .filter(lambda col: col.primary_key)
            .map(lambda col: col.name)
            .to_list()
        )
        cls._unique_keys = (
            cls.columns()
            .filter(lambda col: col.unique)
            .map(lambda col: col.name)
            .to_list()
        )
        cls._constraint_keys: pc.Iter[str] = (
            cls._primary_keys.concat(cls._unique_keys.unwrap()).unique().to_list()
        )
        return cls

    @classmethod
    def _set_schema(cls) -> type[Self]:
        """
        Initializes the schema by collecting columns from parent classes
        and then adding the columns from the current class, preserving order.
        """
        final_schema: dict[str, Column] = {}

        # Iterate over the MRO in reverse to build the schema from the base classes downwards
        for base in reversed(cls.mro()):
            if issubclass(base, Schema) and hasattr(base, "_schema"):
                # Use base.__dict__ to get only the columns defined on that specific class
                for name, obj in base.__dict__.items():
                    if getattr(obj, base._entry_type, False) is True:
                        final_schema[name] = obj

        cls._schema = final_schema
        return cls

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
        return cls._primary_keys

    @classmethod
    def unique_keys(cls) -> pc.Iter[str]:
        """The unique key columns of this schema."""
        return cls._unique_keys

    @classmethod
    def constraint_keys(cls) -> pc.Iter[str]:
        """The primary and unique key columns of this schema."""
        return cls._constraint_keys

    @classmethod
    def sql_schema(cls) -> str:
        """
        Generates the SQL schema definition.
        """
        column_definitions: list[str] = []
        for col in cls.columns().into(list):
            definition: str = f'"{col.name}" {col.sql_type}'
            if col.primary_key:
                definition += " "
                definition += KWord.PRIMARY_KEY
            if col.unique:
                definition += " "
                definition += KWord.UNIQUE
            column_definitions.append(definition)

        return ", ".join(column_definitions)

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
                cls.columns().map(lambda col: col.nw_col.cast(col.nw_dtype)).unwrap()
            )
        )

    @classmethod
    def cast_strict_false(cls, df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        """Like `cast`, but with `strict=False`.

        **Warning**

        will only work with polars {Data, Lazy}Frames.
        """
        return df.lazy().select(
            cls.columns()
            .map(lambda c: c.pl_col.cast(c.pl_dtype, strict=False))
            .unwrap()
        )
