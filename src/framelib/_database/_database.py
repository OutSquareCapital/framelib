import contextlib
import functools
from abc import ABC
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Self

import duckdb
import narwhals as nw
import pyochain as pc

from .._core import BaseEntry, Layout
from . import qry
from ._table import DuckFrame, Table

_DDB = ".ddb"


class DataBase(
    Layout[Table], BaseEntry, ABC, contextlib.ContextDecorator, pc.traits.Pipeable
):
    """A DataBase represents a DuckDB database.

    It's a `Schema` of `Table` entries.

    It's itself a `BaseEntry` that can be used as an entry in a `Folder` (thanks to _is_file attribute).

    The connexion to the database is managed via context manager methods (`__enter__` and `__exit__`), or via the `connect` decorator.

    The latter is the recommended, idiomatic way of dealing with database connections in FrameliB.
    """

    _is_connected: bool = False
    _connexion: duckdb.DuckDBPyConnection

    def __call__[**P, R](self, fn: Callable[P, R]) -> Callable[P, R]:
        """Decorator to ensure database connection context for a method.

        Args:
            fn (Callable[P, R]): The method to wrap.

        Returns:
            Callable[P, R]: The wrapped method with connection context.

        Example:
        ```python
        class MyDatabase(DataBase):
            table1: Table
            table2: Table


        db = MyDatabase()


        @db
        def my_func() -> None:
            db.table1.insert(...)
            db.table2.update(...)


        my_func()
        ```
        """

        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with self:
                return fn(*args, **kwargs)

        return wrapper

    def __set_source__(self, source: Path) -> None:
        self.__source__ = Path(source, self._name).with_suffix(_DDB)
        return (
            self.schema()
            .values()
            .iter()
            .for_each(
                lambda table: table.__set_source__(self.__source__),
            )
        )

    def __enter__(self) -> Self:
        """Enters the context manager, opening the connection to the database.

        Returns:
            Self: The database instance.
        """
        if not self._is_connected:
            self._connexion = duckdb.connect(self.source)
            (
                self.schema()
                .values()
                .iter()
                .for_each(lambda table: table.__set_connexion__(self._connexion))
            )
            self._is_connected = True

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        """Exits the context manager, closing the connection to the database."""
        self._connexion.close()
        self._is_connected = False

    def __del__(self) -> None:
        with contextlib.suppress(Exception):
            self._connexion.close()

    def connect(self) -> Self:
        """Manually connects to the database.

        Warning:
            It's recommended to use the instance as a function decorator instead of this method.

        Returns:
            Self: The database instance.
        """
        return self.__enter__()

    def close(self) -> None:
        """Manually closes the connection to the database.

        It is **highly** recommended to call this method after having called `connect()` explicitly.

        Warning:
            It's recommended to use the instance as a function decorator instead of this method.
        """
        return self.__exit__()

    @property
    def is_connected(self) -> bool:
        """Check if the database is connected.

        Returns:
            bool: True if the database is connected, False otherwise.
        """
        return self._is_connected

    @property
    def connexion(self) -> duckdb.DuckDBPyConnection:
        """Returns the DuckDB connexion."""
        return self._connexion

    def sql(self, sql_query: str) -> DuckFrame:
        """Executes a `SQL` *query* and returns the result.

        Args:
            sql_query (str): The `SQL` *query* to execute.

        Returns:
            DuckFrame: The result of the *query* as a Narwhals `LazyFrame`.
        """
        return nw.from_native(self._connexion.sql(sql_query))

    def show_tables(self) -> DuckFrame:
        """Shows all tables in the database.

        Returns:
            DuckFrame: The tables as a Narwhals LazyFrame.
        """
        return self.sql(qry.SHOW_TABLES)

    def show_views(self) -> DuckFrame:
        """Shows all views in the database.

        Returns:
            DuckFrame: The views as a Narwhals LazyFrame.
        """
        return self.sql(qry.SHOW_VIEWS)

    def show_types(self) -> DuckFrame:
        """Shows all data types, including user-defined ENUMs.

        Returns:
            DuckFrame: The data types as a Narwhals LazyFrame.
        """
        return self.sql(qry.SHOW_TYPES)

    def show_schemas(self) -> DuckFrame:
        """Shows all schemas in the database.

        Returns:
            DuckFrame: The schemas as a Narwhals LazyFrame.
        """
        return self.sql(qry.SHOW_SCHEMAS)

    def show_settings(self) -> DuckFrame:
        """Shows all settings in the current database session.

        Returns:
            DuckFrame: The settings as a Narwhals LazyFrame.
        """
        return self.sql(qry.SHOW_SETTINGS)

    def show_extensions(self) -> DuckFrame:
        """Shows all installed and loaded extensions.

        Returns:
            DuckFrame: The extensions as a Narwhals LazyFrame.
        """
        return self.sql(qry.SHOW_EXTENSIONS)

    def show_all_constraints(self) -> DuckFrame:
        """Shows all constraints across all tables in the database.

        Returns:
            DuckFrame: The constraints as a Narwhals LazyFrame.
        """
        return self.sql(qry.ALL_CONSTRAINTS)

    def sync_schema(self) -> Self:
        """Drops tables from the database that are not present in the schema.

        Returns:
            Self: The database instance.
        """
        self._drop_tables()

        return self

    def _drop_tables(self) -> None:
        return (
            self.show_tables()
            .collect()
            .pipe(lambda df: pc.Set[str](df.get_column("name")))
            .difference(self.schema().keys())
            .iter()
            .for_each(lambda q: self.connexion.execute(qry.drop_table(q)))
        )

    @property
    def source(self) -> Path:
        """Gets the source `Path` of the database."""
        return self.__source__
