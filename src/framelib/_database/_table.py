from typing import TYPE_CHECKING, Self

import duckdb
import narwhals as nw

from .._core import Entry
from . import qry
from ._constraints import on_conflict

if TYPE_CHECKING:
    import polars as pl
    from narwhals.typing import IntoFrame, IntoFrameT, IntoLazyFrame, IntoLazyFrameT

    from ._schema import Schema

type DuckFrame = nw.LazyFrame[duckdb.DuckDBPyRelation]
"""Syntactic sugar for `narwhals.LazyFrame[duckdb.DuckDBPyRelation]`"""


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

    def __set_connexion__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con: duckdb.DuckDBPyConnection = con

    @property
    def relation(self) -> duckdb.DuckDBPyRelation:
        """Get the `duckdb.DuckDBPyRelation` of the table."""
        return self._con.table(self._name)

    def read(self) -> pl.DataFrame:
        """Reads the entire table from the database.

        Returns:
            pl.DataFrame: The table as a Polars DataFrame.
        """
        return self.relation.pl()

    def scan(self) -> DuckFrame:
        """Scan the table from the database.

        Returns:
            DuckFrame: The table as a Narwhals LazyFrame.
        """
        return nw.from_native(self.relation)

    def scan_cast(self) -> DuckFrame:
        """Scan the table from the database and cast it to the schema.

        Returns:
            DuckFrame: The table as a Narwhals LazyFrame.
        """
        return self.scan().pipe(self.model.cast)

    def create_or_replace_from(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """Creates or replaces the table from the dataframe.

        Defines the schema and constraints directly from the model.

        Args:
            df (IntoFrame | IntoLazyFrame): The dataframe to create or replace the table from.

        Returns:
            Self: The table instance.
        """
        _ = _from_df(self.model, df)
        create_q = qry.create_or_replace(self._name, self.model.sql_schema())
        self._con.execute(create_q).execute(qry.insert_into(self._name))

        return self

    def create_from(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """Creates the table from the dataframe.

        Fails if the table already exists.

        Args:
            df (IntoFrame | IntoLazyFrame): The dataframe to create the table from.

        Returns:
            Self: The table instance.
        """
        _ = _from_df(self.model, df)
        self._con.execute(qry.create_from(self._name))
        return self

    def truncate(self) -> Self:
        """Removes all rows from the table.

        Returns:
            Self: The table instance.
        """
        self._con.execute(qry.truncate(self._name))
        return self

    def drop(self) -> Self:
        """Drops the table from the database.

        Returns:
            Self: The table instance.
        """
        self._con.execute(qry.drop(self._name))
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
        self._con.execute(qry.insert_into(self._name))
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
        q = self.model.constraints().map_or(
            lambda kc: qry.insert_on_conflict_update(
                self._name,
                *on_conflict(kc.conflict_keys, self.model.schema()),
            ),
            qry.insert_or_replace(self._name),
        )
        self._con.execute(q)
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
        self._con.execute(qry.insert_or_ignore(self._name))
        return self

    def summarize(self) -> DuckFrame:
        """Summarizes the table, returning statistics about its columns.

        Returns:
            DuckFrame: The summary as a Narwhals LazyFrame.
        """
        return nw.from_native(self._con.sql(qry.summarize(self._name)))

    def describe_columns(self) -> DuckFrame:
        """Returns detailed information about the columns of this table from the INFORMATION_SCHEMA.

        Returns:
            DuckFrame: The columns information as a Narwhals LazyFrame.
        """
        return nw.from_native(self._con.sql(qry.columns_schema(self._name))).select(
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
        return nw.from_native(self._con.sql(qry.constraints(self._name)))
