from __future__ import annotations

from abc import ABC
from collections.abc import Callable
from pathlib import Path
from typing import Any, Concatenate, Final, Self

import duckdb
import narwhals as nw
import pyochain as pc

from .._core import BaseEntry, BaseLayout, EntryType
from ._queries import DBQueries, drop_table
from ._table import DuckFrame, Table

_DDB = ".ddb"


class DataBase(BaseLayout[Table[Any]], BaseEntry, ABC):
    _is_file: Final[bool] = True
    _is_connected: bool = False
    _connexion: duckdb.DuckDBPyConnection
    __entry_type__ = EntryType.TABLE
    _source: Path

    def _connect(self) -> None:
        """Opens the connection to the database."""
        if not self._is_connected:
            self._connexion = duckdb.connect(self.source)
            self._set_tables_connexion()
            self._is_connected = True
        return

    def _set_tables_connexion(self) -> None:
        return (
            self.schema()
            .iter_values()
            .for_each(lambda table: table.__set_connexion__(self._connexion))
        )

    def close(self) -> None:
        """Closes the connection to the database."""
        self._is_connected = False
        self._connexion.close()

    def apply[**P](
        self, fn: Callable[Concatenate[Self, P], Any], *args: P.args, **kwargs: P.kwargs
    ) -> Self:
        """
        Execute a function that takes the database instance and returns self for chaining.

        Allow passing additional arguments to the function.
        Args:
            fn (Callable[Concatenate[Self, P], Any]): The function to execute.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.
        Returns:
            Self: The database instance.
        """
        self._connect()
        fn(self, *args, **kwargs)

        return self

    def pipe[**P, R](
        self, fn: Callable[Concatenate[Self, P], R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """
        Execute a function that takes the database instance and returns the result.

        Allow passing additional arguments to the function.
        Args:
            fn (Callable[Concatenate[Self, P], R]): The function to execute.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.
        Returns:
            R: The result of the function.
        """
        self._connect()

        return fn(self, *args, **kwargs)

    def __set_source__(self, source: Path) -> None:
        self._source = Path(source, self._name).with_suffix(_DDB)
        for table in self._schema.values():
            table.source = self.source

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
        Executes a SQL query and returns the result.
        Args:
            sql_query (str): The SQL query to execute.
        Returns:
            DuckFrame: The result of the query as a Narwhals LazyFrame.
        """
        self._connect()
        return nw.from_native(self._connexion.sql(sql_query))

    def show_tables(self) -> DuckFrame:
        """
        Shows all tables in the database.
        Returns:
            DuckFrame: The tables as a Narwhals LazyFrame.
        """
        return self.query(DBQueries.SHOW_TABLES)

    def show_views(self) -> DuckFrame:
        """Shows all views in the database.
        Returns:
            DuckFrame: The views as a Narwhals LazyFrame.
        """
        return self.query(DBQueries.SHOW_VIEWS)

    def show_types(self) -> DuckFrame:
        """
        Shows all data types, including user-defined ENUMs.
        Returns:
            DuckFrame: The data types as a Narwhals LazyFrame.
        """
        return self.query(DBQueries.SHOW_TYPES)

    def show_schemas(self) -> DuckFrame:
        """
        Shows all schemas in the database.
        Returns:
            DuckFrame: The schemas as a Narwhals LazyFrame.
        """
        return self.query(DBQueries.SHOW_SCHEMAS)

    def show_settings(self) -> DuckFrame:
        """Shows all settings in the current database session.
        Returns:
            DuckFrame: The settings as a Narwhals LazyFrame.
        """
        return self.query(DBQueries.SHOW_SETTINGS)

    def show_extensions(self) -> DuckFrame:
        """Shows all installed and loaded extensions.
        Returns:
            DuckFrame: The extensions as a Narwhals LazyFrame.
        """
        return self.query(DBQueries.SHOW_EXTENSIONS)

    def show_all_constraints(self) -> DuckFrame:
        """Shows all constraints across all tables in the database.
        Returns:
            DuckFrame: The constraints as a Narwhals LazyFrame.
        """
        return self.query(DBQueries.ALL_CONSTRAINTS)

    def sync_schema(self) -> Self:
        """
        Drops tables from the database that are not present in the schema.
        Returns:
            Self: The database instance.
        """
        self._connect()
        self._drop_tables()

        return self

    def _drop_tables(self) -> None:
        tables_in_db = self.show_tables().collect().get_column("name").to_list()
        return (
            pc.Seq(tables_in_db)
            .diff_unique(self.schema().iter_keys().inner())
            .iter()
            .for_each(lambda qry: self.connexion.execute(drop_table(qry)))
        )

    @property
    def source(self) -> Path:
        """
        Returns:
            Path: The source path of the database.
        """
        return self._source
