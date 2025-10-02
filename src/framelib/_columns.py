from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Final

import narwhals as nw

from ._core import BaseEntry


class Column(BaseEntry):
    _is_column: Final[bool] = True
    primary_key: bool

    def __init__(self, primary_key: bool = False) -> None:
        self.primary_key = primary_key
        super().__init__()

    def __repr__(self) -> str:
        return f"Column(name={self.name}, dtype={self.dtype})"

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


class Int128(Column):
    @property
    def dtype(self) -> nw.Int128:
        return nw.Int128()


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


class UInt128(Column):
    @property
    def dtype(self) -> nw.UInt128:
        return nw.UInt128()


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


class DateTime(Column):
    @property
    def dtype(self) -> nw.Time:
        return nw.Time()


class Decimal(Column):
    @property
    def dtype(self) -> nw.Decimal:
        return nw.Decimal()


class Array(Column):
    _inner: nw.dtypes.DType
    _shape: int | tuple[int, ...]

    def __init__(self, inner: nw.dtypes.DType, shape: int | tuple[int, ...]) -> None:
        self._inner = inner
        self._shape = shape

    @property
    def dtype(self) -> nw.Array:
        return nw.Array(self._inner, self._shape)


class Struct(Column):
    _fields: Sequence[nw.Field] | Mapping[str, nw.dtypes.DType]

    def __init__(
        self, fields: Sequence[nw.Field] | Mapping[str, nw.dtypes.DType]
    ) -> None:
        self._fields = fields

    @property
    def dtype(self) -> nw.Struct:
        return nw.Struct(self._fields)


class List(Column):
    _inner: nw.dtypes.DType

    def __init__(self, inner: nw.dtypes.DType) -> None:
        self._inner = inner

    @property
    def dtype(self) -> nw.List:
        return nw.List(self._inner)


class Enum(Column):
    _enum: nw.Enum

    def __init__(self, categories: Iterable[str]) -> None:
        self._enum = nw.Enum(categories)

    @property
    def dtype(self) -> nw.Enum:
        return self._enum
