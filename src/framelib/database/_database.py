from __future__ import annotations

from typing import Any, Final, Self

import duckdb
import narwhals as nw
from narwhals.typing import IntoFrame, IntoLazyFrame

from .._core import BaseLayout, Entry, EntryType
from ..schemas import Schema


class Table[T: Schema](Entry[T, duckdb.DuckDBPyConnection]):
    _is_table: Final[bool] = True

    def with_connexion(self, con: duckdb.DuckDBPyConnection) -> Self:
        self._con = con
        return self

    def read(self) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
        return nw.from_native(self._con.table(self._name))

    def write(self, df: IntoFrame | IntoLazyFrame) -> None:
        _ = nw.from_native(df).lazy().collect().to_native()
        self._con.execute(f"CREATE OR REPLACE TABLE {self._name} AS SELECT * FROM _")


class DataBase(BaseLayout[Table[Schema]]):
    _con: duckdb.DuckDBPyConnection
    _is_entry_type = EntryType.TABLE
    __slots__ = ("_con",)

    def __init__(self) -> None:
        self._con = self.connect()
        for table in self._schema.values():
            table.with_connexion(self._con)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._con.close()

    def __del__(self) -> None:
        self._con.close()

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.__class__.source())
