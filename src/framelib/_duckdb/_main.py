from __future__ import annotations

from pathlib import Path
from typing import Any, Self, TypeGuard

import duckdb
import narwhals as nw
import pychain as pc
from narwhals.typing import IntoFrame, IntoLazyFrame


class Table:
    _name: str
    _con: duckdb.DuckDBPyConnection
    _is_table: bool = True
    __slots__ = ("_name", "_con")

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[Table]:
        return getattr(obj, "_is_table", False)

    def __from_db__(self, name: str) -> None:
        self._name = name

    def with_connexion(self, con: duckdb.DuckDBPyConnection) -> Self:
        self._con = con
        return self

    def read(self) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
        return nw.from_native(self._con.table(self._name))

    @property
    def name(self) -> str:
        return self._name

    def write(self, df: IntoFrame | IntoLazyFrame) -> None:
        df_to_write = nw.from_native(df).lazy().collect().to_native()  # type: ignore # noqa
        self._con.execute(
            f"CREATE OR REPLACE TABLE {self._name} AS SELECT * FROM df_to_write"
        )


class DataBase:
    __directory__: Path
    _is_db: bool = True
    _schema: dict[str, Table]
    _con: duckdb.DuckDBPyConnection
    __slots__ = ("_con",)

    def __init__(self) -> None:
        self._set_connexion()

    def __init_subclass__(cls) -> None:
        cls._set_dir()._set_schema()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.connexion.close()

    def __del__(self) -> None:
        self._con.close()

    @classmethod
    def _set_schema(cls) -> type[Self]:
        cls._schema = {}
        for name, obj in cls.__dict__.items():
            if Table.__identity__(obj):
                obj.__from_db__(name)
                cls._schema[name] = obj
            else:
                continue
        return cls

    @classmethod
    def _set_dir(cls) -> type[Self]:
        if not hasattr(cls, "__directory__"):
            cls.__directory__ = Path()
        cls.__directory__ = (
            cls.directory().joinpath(cls.__name__.lower()).with_suffix(".db")
        )
        return cls

    def _set_connexion(self) -> Self:
        self._con = duckdb.connect(self.directory())
        for table in self._schema.values():
            table.with_connexion(self._con)
        return self

    @classmethod
    def directory(cls) -> Path:
        return cls.__directory__

    @property
    def connexion(self) -> duckdb.DuckDBPyConnection:
        return self._con

    @property
    def schema(self) -> pc.Dict[str, Table]:
        return pc.Dict(self._schema)
