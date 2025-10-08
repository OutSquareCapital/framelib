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
    _is_connected: bool = False
    _connexion: duckdb.DuckDBPyConnection
    _entry_type = EntryType.TABLE
    _source: Path
    _model: pc.Dict[str, Table]

    def _connect(self) -> Self:
        """Opens the connection to the database and returns self for chaining."""
        if not self._is_connected:
            self._connexion = duckdb.connect(self.source)
            for table in self._schema.values():
                table.__from_connexion__(self._connexion)
            self._is_connected = True
        return self

    def close(self) -> None:
        """Closes the connection to the database."""
        self._is_connected = False
        self._connexion.close()

    def pipe[**P](
        self, fn: Callable[Concatenate[Self, P], Any], *args: P.args, **kwargs: P.kwargs
    ) -> Self:
        """
        Execute a function that takes the database instance and returns self for chaining.

        Allow passing additional arguments to the function.
        """
        self._connect()
        fn(self, *args, **kwargs)
        return self

    def apply(self, *fn: Callable[[Self], Any]) -> Self:
        """Execute multiples functions with the instance and returns self for chaining."""
        self._connect()
        for f in fn:
            f(self)
        return self

    def __from_source__(self, source: Path) -> None:
        self._source = Path(source, self._name).with_suffix(_DDB)
        self._model = self.schema()
        for table in self._schema.values():
            table.source = self.source

    def __enter__(self) -> Self:
        return self._connect()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
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
