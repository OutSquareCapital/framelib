from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Final, TypeGuard

import narwhals as nw


class Column:
    _is_column: Final[bool] = True
    _name: str
    __slots__ = "_name"

    def __repr__(self) -> str:
        return f"Column(name={self.name}, dtype={self.dtype})"

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[Column]:
        return getattr(obj, "_is_column", False) is True

    def __from_schema__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def col(self) -> nw.Expr:
        return nw.col(self._name)

    @property
    def dtype(self) -> nw.dtypes.DType:
        raise NotImplementedError


class Float32(Column):
    @property
    def dtype(self) -> nw.Float32:
        return nw.Float32()


class Float64(Column):
    @property
    def dtype(self) -> nw.Float64:
        return nw.Float64()


class Int8(Column):
    @property
    def dtype(self) -> nw.Int8:
        return nw.Int8()


class Int16(Column):
    @property
    def dtype(self) -> nw.Int16:
        return nw.Int16()


class Int32(Column):
    @property
    def dtype(self) -> nw.Int32:
        return nw.Int32()


class Int64(Column):
    @property
    def dtype(self) -> nw.Int64:
        return nw.Int64()


class UInt8(Column):
    @property
    def dtype(self) -> nw.UInt8:
        return nw.UInt8()


class UInt16(Column):
    @property
    def dtype(self) -> nw.UInt16:
        return nw.UInt16()


class UInt32(Column):
    @property
    def dtype(self) -> nw.UInt32:
        return nw.UInt32()


class UInt64(Column):
    @property
    def dtype(self) -> nw.UInt64:
        return nw.UInt64()


class Boolean(Column):
    @property
    def dtype(self) -> nw.Boolean:
        return nw.Boolean()


class String(Column):
    @property
    def dtype(self) -> nw.String:
        return nw.String()


class Date(Column):
    @property
    def dtype(self) -> nw.Date:
        return nw.Date()


class Categorical(Column):
    @property
    def dtype(self) -> nw.Categorical:
        return nw.Categorical()


class Enum(Column):
    _enum: nw.Enum

    def __init__(self, categories: Iterable[str]) -> None:
        self._enum = nw.Enum(categories)

    @property
    def dtype(self) -> nw.Enum:
        return self._enum
