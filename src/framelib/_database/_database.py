from __future__ import annotations

from abc import ABC
from collections.abc import Callable
from pathlib import Path
from typing import Any, Concatenate, Final, Self

import duckdb
import narwhals as nw
import pychain as pc

from .._core import BaseEntry, BaseLayout, EntryType
from ._queries import DBQueries
from ._table import DuckFrame, Table

_DDB = ".ddb"


class DataBase(BaseLayout[Table], BaseEntry, ABC):
    _is_file: Final[bool] = True
    _connexion: duckdb.DuckDBPyConnection
    _entry_type = EntryType.TABLE
    _source: Path
    _model: pc.Dict[str, Table]

    def pipe[**P, R](
        self, fn: Callable[Concatenate[Self, P], R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Execute a function with the connection opened and returns its result."""
        with self:
            return fn(self, *args, **kwargs)

    def apply[**P](
        self, fn: Callable[Concatenate[Self, P], Any], *args: P.args, **kwargs: P.kwargs
    ) -> Self:
        """Execute a function with the connection opened and returns self for chaining."""
        with self:
            fn(self, *args, **kwargs)
        return self

    def __from_source__(self, source: Path) -> None:
        self._source = Path(source, self._name).with_suffix(_DDB)
        self._model = self.schema()
        for table in self._schema.values():
            table.source = self.source

    def __enter__(self) -> Self:
        self._connexion = duckdb.connect(self.source)
        for table in self._schema.values():
            table.__from_connexion__(self._connexion)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._connexion.close()

    def __del__(self) -> None:
        try:
            self._connexion.close()
        except Exception:
            pass

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
        return self.query(DBQueries.SHOW_TABLES)

    def show_views(self) -> DuckFrame:
        """Shows all views in the database."""
        return self.query(DBQueries.SHOW_VIEWS)

    def show_types(self) -> DuckFrame:
        """
        Shows all data types, including user-defined ENUMs.
        """
        return self.query(DBQueries.SHOW_TYPES)

    def show_schemas(self) -> DuckFrame:
        """
        Shows all schemas in the database.
        """
        return self.query(DBQueries.SHOW_SCHEMAS)

    def show_settings(self) -> DuckFrame:
        """Shows all settings in the current database session."""
        return self.query(DBQueries.SHOW_SETTINGS)

    def show_extensions(self) -> DuckFrame:
        """Shows all installed and loaded extensions."""
        return self.query(DBQueries.SHOW_EXTENSIONS)

    def show_all_constraints(self) -> DuckFrame:
        """Shows all constraints across all tables in the database."""
        return self.query(DBQueries.ALL_CONSTRAINTS)

    @property
    def source(self) -> Path:
        """Returns the source path of the database."""
        return self._source

    @property
    def model(self) -> pc.Dict[str, Table]:
        """Returns the model of the database."""
        return self._model
