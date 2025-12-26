import contextlib
from abc import ABC
from collections.abc import Callable
from pathlib import Path
from typing import Any, Concatenate, Self

import duckdb
import narwhals as nw
import pyochain as pc

from .._core import BaseEntry, Layout
from . import qry
from ._table import DuckFrame, Table

_DDB = ".ddb"


class DataBase(Layout[Table], BaseEntry, ABC):
    """A DataBase represents a DuckDB database.

    It's a `Schema` of `Table` entries.

    It's itself a `BaseEntry` that can be used as an entry in a `Folder` (thanks to _is_file attribute).
    """

    _is_connected: bool = False
    _connexion: duckdb.DuckDBPyConnection

    def _connect(self) -> None:
        """Opens the connection to the database.

        Returns:
            None: The connection to the database is opened.
        """
        if not self._is_connected:
            self._connexion = duckdb.connect(self.source)
            self._set_tables_connexion()
            self._is_connected = True

    def _set_tables_connexion(self) -> None:
        return (
            self.schema()
            .values_iter()
            .for_each(lambda table: table.__set_connexion__(self._connexion))
        )

    def close(self) -> None:
        """Closes the connection to the database."""
        self._is_connected = False
        self._connexion.close()

    def apply[**P](
        self,
        fn: Callable[Concatenate[Self, P], Any],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Self:
        """Execute a function that takes the database instance and returns self for chaining.

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
        self,
        fn: Callable[Concatenate[Self, P], R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Execute a function that takes the database instance and returns the result.

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
        self.__source__ = Path(source, self._name).with_suffix(_DDB)
        return (
            self.schema()
            .values_iter()
            .for_each(
                lambda table: table.__set_source__(self.__source__),
            )
        )

    def __del__(self) -> None:
        with contextlib.suppress(Exception):
            self.close()

    @property
    def connexion(self) -> duckdb.DuckDBPyConnection:
        """Returns the DuckDB connexion."""
        return self._connexion

    def query(self, sql_query: str) -> DuckFrame:
        """Executes a SQL query and returns the result.

        Args:
            sql_query (str): The SQL query to execute.

        Returns:
            DuckFrame: The result of the query as a Narwhals LazyFrame.
        """
        self._connect()
        return nw.from_native(self._connexion.sql(sql_query))

    def show_tables(self) -> DuckFrame:
        """Shows all tables in the database.

        Returns:
            DuckFrame: The tables as a Narwhals LazyFrame.
        """
        return self.query(qry.SHOW_TABLES)

    def show_views(self) -> DuckFrame:
        """Shows all views in the database.

        Returns:
            DuckFrame: The views as a Narwhals LazyFrame.
        """
        return self.query(qry.SHOW_VIEWS)

    def show_types(self) -> DuckFrame:
        """Shows all data types, including user-defined ENUMs.

        Returns:
            DuckFrame: The data types as a Narwhals LazyFrame.
        """
        return self.query(qry.SHOW_TYPES)

    def show_schemas(self) -> DuckFrame:
        """Shows all schemas in the database.

        Returns:
            DuckFrame: The schemas as a Narwhals LazyFrame.
        """
        return self.query(qry.SHOW_SCHEMAS)

    def show_settings(self) -> DuckFrame:
        """Shows all settings in the current database session.

        Returns:
            DuckFrame: The settings as a Narwhals LazyFrame.
        """
        return self.query(qry.SHOW_SETTINGS)

    def show_extensions(self) -> DuckFrame:
        """Shows all installed and loaded extensions.

        Returns:
            DuckFrame: The extensions as a Narwhals LazyFrame.
        """
        return self.query(qry.SHOW_EXTENSIONS)

    def show_all_constraints(self) -> DuckFrame:
        """Shows all constraints across all tables in the database.

        Returns:
            DuckFrame: The constraints as a Narwhals LazyFrame.
        """
        return self.query(qry.ALL_CONSTRAINTS)

    def sync_schema(self) -> Self:
        """Drops tables from the database that are not present in the schema.

        Returns:
            Self: The database instance.
        """
        self._connect()
        self._drop_tables()

        return self

    def _drop_tables(self) -> None:
        return (
            pc.Iter[str](self.show_tables().collect().get_column("name"))
            .collect(pc.Set)
            .difference(self.schema().keys_iter().collect(pc.Set))
            .iter()
            .for_each(lambda q: self.connexion.execute(qry.drop_table(q)))
        )

    @property
    def source(self) -> Path:
        """Gets the source `Path` of the database."""
        return self.__source__
