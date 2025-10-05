from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import Any, Final, Self

import duckdb
import narwhals as nw
import pychain as pc

from .._core import BaseEntry, BaseLayout, EntryType
from . import _queries as qry
from ._table import DuckFrame, Table

_DDB = ".ddb"


class DataBase(BaseLayout[Table], BaseEntry, ABC):
    _is_file: Final[bool] = True
    _connexion: duckdb.DuckDBPyConnection
    _entry_type = EntryType.TABLE
    _source: Path
    _model: pc.Dict[str, Table]

    def __from_source__(self, source: Path) -> None:
        self._source = Path(source, self._name).with_suffix(_DDB)
        self._model = self.schema()
        for table in self._schema.values():
            table.source = self.source

    def __enter__(self) -> Self:
        self._connexion = duckdb.connect(self.source)
        for table in self._schema.values():
            table.with_connexion(self._connexion)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._connexion.close()

    @property
    def connexion(self) -> duckdb.DuckDBPyConnection:
        """Returns the DuckDB connexion."""
        return self._connexion

    def query(self, sql_query: str) -> DuckFrame:
        """
        Executes a SQL query and returns the result as a DuckDB relation wrapped in a Narwhals LazyFrame.
        """
        return nw.from_native(self._connexion.sql(sql_query))

    def show_tables(self) -> DuckFrame:
        """
        Shows all tables in the database.
        """
        return self.query(qry.show_tables())

    def show_types(self) -> DuckFrame:
        """
        Shows all data types, including user-defined ENUMs.
        """
        return self.query(qry.show_types())

    def show_schemas(self) -> DuckFrame:
        """
        Shows all schemas in the database.
        """
        return self.query(qry.show_schemas())

    @property
    def source(self) -> Path:
        """Returns the source path of the database."""
        return self._source

    @property
    def model(self) -> pc.Dict[str, Table]:
        """Returns the model of the database."""
        return self._model
