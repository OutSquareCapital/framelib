from __future__ import annotations

import narwhals as nw
import polars as pl

from ._base import Column


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
