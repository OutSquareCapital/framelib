from __future__ import annotations

from dataclasses import dataclass

import narwhals as nw
import polars as pl

from ._base import Column


@dataclass(slots=True)
class Boolean(Column):
    @property
    def nw_dtype(self) -> nw.Boolean:
        return nw.Boolean()

    @property
    def pl_dtype(self) -> pl.Boolean:
        return pl.Boolean()

    @property
    def sql_type(self) -> str:
        return "BOOLEAN"


@dataclass(slots=True)
class String(Column):
    @property
    def nw_dtype(self) -> nw.String:
        return nw.String()

    @property
    def pl_dtype(self) -> pl.String:
        return pl.String()

    @property
    def sql_type(self) -> str:
        return "VARCHAR"


@dataclass(slots=True)
class Date(Column):
    @property
    def nw_dtype(self) -> nw.Date:
        return nw.Date()

    @property
    def pl_dtype(self) -> pl.Date:
        return pl.Date()

    @property
    def sql_type(self) -> str:
        return "DATE"


@dataclass(slots=True)
class Float32(Column):
    @property
    def nw_dtype(self) -> nw.Float32:
        return nw.Float32()

    @property
    def pl_dtype(self) -> pl.Float32:
        return pl.Float32()

    @property
    def sql_type(self) -> str:
        return "FLOAT"


@dataclass(slots=True)
class Float64(Column):
    @property
    def nw_dtype(self) -> nw.Float64:
        return nw.Float64()

    @property
    def pl_dtype(self) -> pl.Float64:
        return pl.Float64()

    @property
    def sql_type(self) -> str:
        return "DOUBLE"


@dataclass(slots=True)
class Int8(Column):
    @property
    def nw_dtype(self) -> nw.Int8:
        return nw.Int8()

    @property
    def pl_dtype(self) -> pl.Int8:
        return pl.Int8()

    @property
    def sql_type(self) -> str:
        return "TINYINT"


@dataclass(slots=True)
class Int16(Column):
    @property
    def nw_dtype(self) -> nw.Int16:
        return nw.Int16()

    @property
    def pl_dtype(self) -> pl.Int16:
        return pl.Int16()

    @property
    def sql_type(self) -> str:
        return "SMALLINT"


@dataclass(slots=True)
class Int32(Column):
    @property
    def nw_dtype(self) -> nw.Int32:
        return nw.Int32()

    @property
    def pl_dtype(self) -> pl.Int32:
        return pl.Int32()

    @property
    def sql_type(self) -> str:
        return "INTEGER"


@dataclass(slots=True)
class Int64(Column):
    @property
    def nw_dtype(self) -> nw.Int64:
        return nw.Int64()

    @property
    def pl_dtype(self) -> pl.Int64:
        return pl.Int64()

    @property
    def sql_type(self) -> str:
        return "BIGINT"


@dataclass(slots=True)
class Int128(Column):
    @property
    def nw_dtype(self) -> nw.Int128:
        return nw.Int128()

    @property
    def pl_dtype(self) -> pl.Int128:
        return pl.Int128()

    @property
    def sql_type(self) -> str:
        return "HUGEINT"


@dataclass(slots=True)
class UInt8(Column):
    @property
    def nw_dtype(self) -> nw.UInt8:
        return nw.UInt8()

    @property
    def pl_dtype(self) -> pl.UInt8:
        return pl.UInt8()

    @property
    def sql_type(self) -> str:
        return "UTINYINT"


@dataclass(slots=True)
class UInt16(Column):
    @property
    def nw_dtype(self) -> nw.UInt16:
        return nw.UInt16()

    @property
    def pl_dtype(self) -> pl.UInt16:
        return pl.UInt16()

    @property
    def sql_type(self) -> str:
        return "USMALLINT"


@dataclass(slots=True)
class UInt32(Column):
    @property
    def nw_dtype(self) -> nw.UInt32:
        return nw.UInt32()

    @property
    def pl_dtype(self) -> pl.UInt32:
        return pl.UInt32()

    @property
    def sql_type(self) -> str:
        return "UINTEGER"


@dataclass(slots=True)
class UInt64(Column):
    @property
    def nw_dtype(self) -> nw.UInt64:
        return nw.UInt64()

    @property
    def pl_dtype(self) -> pl.UInt64:
        return pl.UInt64()

    @property
    def sql_type(self) -> str:
        return "UBIGINT"


@dataclass(slots=True)
class UInt128(Column):
    @property
    def nw_dtype(self) -> nw.UInt128:
        return nw.UInt128()

    @property
    def pl_dtype(self) -> pl.UInt128:
        return pl.UInt128()

    @property
    def sql_type(self) -> str:
        return "UHUGEINT"
