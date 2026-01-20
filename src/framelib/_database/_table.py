from __future__ import annotations

from typing import TYPE_CHECKING, Self

import narwhals as nw
import pyochain as pc
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from .._core import Entry
from . import qry

if TYPE_CHECKING:
    import polars as pl
    from narwhals.typing import IntoFrame, IntoFrameT, IntoLazyFrame, IntoLazyFrameT

    from .._schema import Schema

type DuckFrame = nw.LazyFrame[DuckDBPyRelation]
"""Syntactic sugar for `narwhals.LazyFrame[DuckDBPyRelation]`"""


def _from_df(
    model: type[Schema], df: IntoFrameT | IntoLazyFrameT
) -> IntoFrameT | IntoLazyFrameT:
    """Cast the input frame to the provided `Schema` and return the native frame.

    The caller **must** bind the result to a local variable named "_"
    so `DuckDB` can resolve it in SQL queries like `SELECT * FROM _`.

    See https://duckdb.org/docs/stable/guides/python/sql_on_pandas for details.

    Args:
        model (type[Schema]): The table's schema model.
        df (IntoFrameT | IntoLazyFrameT): The input dataframe.

    Returns:
        IntoFrameT | IntoLazyFrameT: The casted native DataFrame.
    """
    return nw.from_native(df).lazy().pipe(model.cast).to_native()


class Table(Entry):
    """A `Table` represents a DuckDB table whose logical schema is defined by model (a Schema subclass).

    It is an `Entry` in a `DataBase` layout.

    """

    __slots__ = ("_con",)
    _con: DuckDBPyConnection

    def __set_connexion__(self, con: DuckDBPyConnection) -> None:
        self._con: DuckDBPyConnection = con

    @property
    def connexion(self) -> pc.Result[DuckDBPyConnection, RuntimeError]:
        """Get the `DuckDBPyConnection` of the table.

        `Ok(connection)` if the table is connected to a database, `Err(RuntimeError)` otherwise.

        Returns:
            pc.Result[DuckDBPyConnection, RuntimeError]: The connection result.
        """
        try:
            return pc.Ok(self._con)
        except AttributeError:
            msg = "The table is not connected to any database."
            return pc.Err(RuntimeError(msg))

    @property
    def relation(self) -> pc.Result[DuckDBPyRelation, RuntimeError]:
        """Get the `DuckDBPyRelation` of the table.

        Returns:
            pc.Result[DuckDBPyRelation, RuntimeError]: `Ok(relation)` if the table is connected to a database, `Err(RuntimeError)` otherwise.
        """
        return self.connexion.map(lambda c: c.table(self._name))

    def read(self) -> pl.DataFrame:
        """Reads the entire table from the database and materializes it as a **polars DataFrame**.

        This is syntactic sugar for `self.scan().to_native().pl()`.

        Returns:
            pl.DataFrame: The table as a Polars DataFrame.
        """
        return self.relation.unwrap().pl()

    def scan(self) -> DuckFrame:
        """Scan the table from the database.

        Returns:
            DuckFrame: The table as a Narwhals LazyFrame.
        """
        return nw.from_native(self.relation.unwrap())

    def create(self) -> Self:
        """Creates the table in the database.

        Will fail if the table already exists.

        Returns:
            Self: The table instance.
        """
        q = qry.create(self._name, self.model.to_sql())
        self.connexion.unwrap().execute(q)
        return self

    def create_if_not_exist(self) -> Self:
        """Creates the table in the database if it does not already exist.

        If the table exists, this is a no-op.

        Returns:
            Self: The table instance.
        """
        q = qry.create_if_not_exist(self._name, self.model.to_sql())
        self.connexion.unwrap().execute(q)
        return self

    def create_or_replace(self) -> Self:
        """Creates or replaces the table in the database if it already exists.

        Returns:
            Self: The table instance.
        """
        q = qry.create_or_replace(self._name, self.model.to_sql())
        self.connexion.unwrap().execute(q)
        return self

    def truncate(self) -> Self:
        """Removes all rows from the table.

        Returns:
            Self: The table instance.
        """
        self.connexion.unwrap().execute(qry.truncate(self._name))
        return self

    def drop(self) -> Self:
        """Drops the table from the database.

        Will fail if the table does not exist.

        Returns:
            Self: The table instance.
        """
        self.connexion.unwrap().execute(qry.drop(self._name))
        return self

    def drop_if_exist(self) -> Self:
        """Drops the table from the database if it exists.

        Returns:
            Self: The table instance.
        """
        self.connexion.unwrap().execute(qry.drop_if_exists(self._name))
        return self

    def insert_into(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """Appends rows to the table.

        Fails if the table does not exist.

        Args:
            df (IntoFrame | IntoLazyFrame): The dataframe to insert into the table.

        Returns:
            Self: The table instance.
        """
        _ = _from_df(self.model, df)
        self.connexion.unwrap().execute(qry.insert_into(self._name))
        return self

    def insert_or_replace(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """Inserts rows from the dataframe.

        Replace any rows that cause a primary key conflict.

        Args:
            df (IntoFrame | IntoLazyFrame): The dataframe to insert or replace.

        Returns:
            Self: The table instance.
        """
        _ = _from_df(self.model, df)
        self.connexion.unwrap().execute(qry.insert_or_replace(self._name))
        return self

    def insert_or_ignore(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """Inserts rows from the dataframe.

        Ignore any rows that cause a primary key conflict.

        Args:
            df (IntoFrame | IntoLazyFrame): The dataframe to insert or ignore.

        Returns:
            Self: The table instance.
        """
        _ = _from_df(self.model, df)
        self.connexion.unwrap().execute(qry.insert_or_ignore(self._name))
        return self

    def summarize(self) -> DuckFrame:
        """Summarizes the table, returning statistics about its columns.

        Returns:
            DuckFrame: The summary as a Narwhals LazyFrame.
        """
        return nw.from_native(self.connexion.unwrap().sql(qry.summarize(self._name)))

    def describe_columns(self) -> DuckFrame:
        """Returns detailed information about the columns of this table from the INFORMATION_SCHEMA.

        Returns:
            DuckFrame: The columns information as a Narwhals LazyFrame.
        """
        q = self.connexion.unwrap().sql(qry.columns_schema(self._name))
        return nw.from_native(q).select(
            "column_name",
            "data_type",
            "is_nullable",
            "column_default",
        )

    def describe_constraints(self) -> DuckFrame:
        """Returns the constraints (PRIMARY KEY, UNIQUE) applied to this table.

        Returns:
            DuckFrame: The constraints information as a Narwhals LazyFrame.
        """
        return nw.from_native(self.connexion.unwrap().sql(qry.constraints(self._name)))
