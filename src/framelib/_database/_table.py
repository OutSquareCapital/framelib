from pathlib import Path
from typing import Final, Self

import duckdb
import narwhals as nw
import polars as pl
from narwhals.typing import IntoFrame, IntoFrameT, IntoLazyFrame, IntoLazyFrameT

from .._core import Entry
from .._schema import Schema
from ._queries import Queries

type DuckFrame = nw.LazyFrame[duckdb.DuckDBPyRelation]
"""Syntactic sugar for narwhals.LazyFrame[duckdb.DuckDBPyRelation]"""


class Table(Entry[Schema, Path]):
    _is_table: Final[bool] = True
    _qry: Queries

    def __from_connexion__(self, con: duckdb.DuckDBPyConnection) -> Self:
        self._con: duckdb.DuckDBPyConnection = con
        self._qry = Queries(self._name)
        return self

    @property
    def relation(self) -> duckdb.DuckDBPyRelation:
        return self._con.table(self._name)

    def _from_df(self, df: IntoFrameT | IntoLazyFrameT) -> IntoFrameT | IntoLazyFrameT:
        return nw.from_native(df).lazy().pipe(self.model.cast).to_native()

    def read(self) -> pl.DataFrame:
        """Reads the entire table from the database and returns it as a Polars DataFrame."""
        return self.relation.pl()

    def scan(self) -> DuckFrame:
        """Scan the table from the database, and returns it as a Narwhals LazyFrame."""
        return nw.from_native(self.relation)

    def scan_cast(self) -> DuckFrame:
        """Scan the table from the database, cast to the schema, and returns it as a Narwhals LazyFrame."""
        return self.scan().pipe(self.model.cast)

    def create_or_replace_from(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Creates or replaces the table from the dataframe, defining the schema
        and constraints directly from the model.
        """
        _ = self._from_df(df)
        create_q = self._qry.create_or_replace(self.model.sql_schema())
        self._con.execute(create_q).execute(self._qry.insert_into())

        return self

    def create_from(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Creates the table from the dataframe.
        Fails if the table already exists.
        """
        _ = self._from_df(df)
        self._con.execute(self._qry.create_from())
        return self

    def truncate(self) -> Self:
        """Removes all rows from the table."""
        self._con.execute(self._qry.truncate())
        return self

    def drop(self) -> Self:
        """Drops the table from the database."""
        self._con.execute(self._qry.drop())
        return self

    def insert_into(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Appends rows to the table.
        Fails if the table does not exist.
        """
        _ = self._from_df(df)
        self._con.execute(self._qry.insert_into())
        return self

    def insert_or_replace(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Inserts rows from the dataframe, replacing any rows that cause a
        primary key conflict (UPSERT).
        """
        _ = self._from_df(df)

        self._con.execute(self._qry.insert_or_replace())
        return self

    def insert_or_ignore(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Inserts rows from the dataframe, ignoring any rows that cause a
        primary key conflict.
        """
        _ = self._from_df(df)
        self._con.execute(self._qry.insert_or_ignore())
        return self

    def summarize(self) -> DuckFrame:
        """Summarizes the table, returning statistics about its columns."""
        return nw.from_native(self._con.sql(self._qry.summarize()))
