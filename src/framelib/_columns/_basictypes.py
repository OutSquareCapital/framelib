from dataclasses import dataclass
from typing import override

import narwhals as nw
import polars as pl

from ._base import Column


@dataclass(slots=True, eq=False)
class Boolean(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Boolean:
        return nw.Boolean()

    @property
    @override
    def pl_dtype(self) -> pl.Boolean:
        return pl.Boolean()

    @property
    @override
    def sql_type(self) -> str:
        return "BOOLEAN"


@dataclass(slots=True, eq=False)
class String(Column):
    @property
    @override
    def nw_dtype(self) -> nw.String:
        return nw.String()

    @property
    @override
    def pl_dtype(self) -> pl.String:
        return pl.String()

    @property
    @override
    def sql_type(self) -> str:
        return "VARCHAR"


@dataclass(slots=True, eq=False)
class Date(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Date:
        return nw.Date()

    @property
    @override
    def pl_dtype(self) -> pl.Date:
        return pl.Date()

    @property
    @override
    def sql_type(self) -> str:
        return "DATE"


@dataclass(slots=True, eq=False)
class Float32(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Float32:
        return nw.Float32()

    @property
    @override
    def pl_dtype(self) -> pl.Float32:
        return pl.Float32()

    @property
    @override
    def sql_type(self) -> str:
        return "FLOAT"


@dataclass(slots=True, eq=False)
class Float64(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Float64:
        return nw.Float64()

    @property
    @override
    def pl_dtype(self) -> pl.Float64:
        return pl.Float64()

    @property
    @override
    def sql_type(self) -> str:
        return "DOUBLE"


@dataclass(slots=True, eq=False)
class Int8(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Int8:
        return nw.Int8()

    @property
    @override
    def pl_dtype(self) -> pl.Int8:
        return pl.Int8()

    @property
    @override
    def sql_type(self) -> str:
        return "TINYINT"


@dataclass(slots=True, eq=False)
class Int16(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Int16:
        return nw.Int16()

    @property
    @override
    def pl_dtype(self) -> pl.Int16:
        return pl.Int16()

    @property
    @override
    def sql_type(self) -> str:
        return "SMALLINT"


@dataclass(slots=True, eq=False)
class Int32(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Int32:
        return nw.Int32()

    @property
    @override
    def pl_dtype(self) -> pl.Int32:
        return pl.Int32()

    @property
    @override
    def sql_type(self) -> str:
        return "INTEGER"


@dataclass(slots=True, eq=False)
class Int64(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Int64:
        return nw.Int64()

    @property
    @override
    def pl_dtype(self) -> pl.Int64:
        return pl.Int64()

    @property
    @override
    def sql_type(self) -> str:
        return "BIGINT"


@dataclass(slots=True, eq=False)
class Int128(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Int128:
        return nw.Int128()

    @property
    @override
    def pl_dtype(self) -> pl.Int128:
        return pl.Int128()

    @property
    @override
    def sql_type(self) -> str:
        return "HUGEINT"


@dataclass(slots=True, eq=False)
class UInt8(Column):
    @property
    @override
    def nw_dtype(self) -> nw.UInt8:
        return nw.UInt8()

    @property
    @override
    def pl_dtype(self) -> pl.UInt8:
        return pl.UInt8()

    @property
    @override
    def sql_type(self) -> str:
        return "UTINYINT"


@dataclass(slots=True, eq=False)
class UInt16(Column):
    @property
    @override
    def nw_dtype(self) -> nw.UInt16:
        return nw.UInt16()

    @property
    @override
    def pl_dtype(self) -> pl.UInt16:
        return pl.UInt16()

    @property
    @override
    def sql_type(self) -> str:
        return "USMALLINT"


@dataclass(slots=True, eq=False)
class UInt32(Column):
    @property
    @override
    def nw_dtype(self) -> nw.UInt32:
        return nw.UInt32()

    @property
    @override
    def pl_dtype(self) -> pl.UInt32:
        return pl.UInt32()

    @property
    @override
    def sql_type(self) -> str:
        return "UINTEGER"


@dataclass(slots=True, eq=False)
class UInt64(Column):
    @property
    @override
    def nw_dtype(self) -> nw.UInt64:
        return nw.UInt64()

    @property
    @override
    def pl_dtype(self) -> pl.UInt64:
        return pl.UInt64()

    @property
    @override
    def sql_type(self) -> str:
        return "UBIGINT"


@dataclass(slots=True, eq=False)
class UInt128(Column):
    @property
    @override
    def nw_dtype(self) -> nw.UInt128:
        return nw.UInt128()

    @property
    @override
    def pl_dtype(self) -> pl.UInt128:
        return pl.UInt128()

    @property
    @override
    def sql_type(self) -> str:
        return "UHUGEINT"
