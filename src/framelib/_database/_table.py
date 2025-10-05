from pathlib import Path
from typing import Final, Self

import duckdb
import narwhals as nw
from narwhals.typing import IntoFrame, IntoFrameT, IntoLazyFrame, IntoLazyFrameT

from .._core import Entry
from .._schema import Schema
from . import _queries as qry

type DuckFrame = nw.LazyFrame[duckdb.DuckDBPyRelation]
"""Syntactic sugar for narwhals.LazyFrame[duckdb.DuckDBPyRelation]"""


class Table(Entry[Schema, Path]):
    _is_table: Final[bool] = True

    def _from_df(self, df: IntoFrameT | IntoLazyFrameT) -> IntoFrameT | IntoLazyFrameT:
        return nw.from_native(df).lazy().pipe(self.model.cast).to_native()

    def with_connexion(self, con: duckdb.DuckDBPyConnection) -> Self:
        """
        Sets the DuckDB connexion for the table.

        Is called automatically when entering a DataBase context.

        Is provided as a convenience method in case you want to use the table outside of a DataBase context, for pure SQL operations for example.
        """
        self._con = con
        return self

    def scan(self) -> DuckFrame:
        """Scan the table from the database, and returns it as a Narwhals LazyFrame."""
        return nw.from_native(self._con.table(self._name))

    def scan_cast(self) -> DuckFrame:
        """Scan the table from the database, cast to the schema, and returns it as a Narwhals LazyFrame."""
        return nw.from_native(self._con.table(self._name)).pipe(self.model.cast)

    def describe(self) -> DuckFrame:
        """Describes the schema of the table."""
        return nw.from_native(self._con.sql(qry.describe(self._name)))

    def create_or_replace_from(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Creates or replaces the table from the dataframe.
        Adds primary key and unique constraints if defined in the schema.
        """
        _ = self._from_df(df)
        self._con.execute(qry.create_or_replace(self._name))

        # Ajout des contraintes PK
        pk_names: list[str] = self.model.primary_keys().pipe_unwrap(list)
        if pk_names:
            self._con.execute(qry.add_primary_key(self._name, *pk_names))

        u_names: list[str] = self.model.unique_keys().pipe_unwrap(list)
        for key in u_names:
            self._con.execute(f"ALTER TABLE {self._name} ADD UNIQUE ({key});")

        return self

    def append(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Appends rows to the table.
        Fails if the table does not exist.
        """
        _ = self._from_df(df)
        self._con.execute(qry.insert_into(self._name))
        return self

    def create_from(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Creates the table from the dataframe.
        Fails if the table already exists.
        """
        _ = self._from_df(df)
        self._con.execute(qry.create_from(self._name))
        return self

    def truncate(self) -> Self:
        """Removes all rows from the table."""
        self._con.execute(qry.truncate(self._name))
        return self

    def drop(self) -> Self:
        """Drops the table from the database."""
        self._con.execute(qry.drop(self._name))
        return self

    def insert_or_replace(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Inserts rows from the dataframe, replacing any rows that cause a
        primary key conflict (UPSERT).
        """
        _ = self._from_df(df)
        self._con.execute(qry.insert_or_replace(self._name))
        return self

    def insert_or_ignore(self, df: IntoFrame | IntoLazyFrame) -> Self:
        """
        Inserts rows from the dataframe, ignoring any rows that cause a
        primary key conflict.
        """
        _ = self._from_df(df)
        self._con.execute(qry.insert_or_ignore(self._name))
        return self

    def summarize(self) -> DuckFrame:
        """Summarizes the table, returning statistics about its columns."""
        return nw.from_native(self._con.sql(qry.summarize(self._name)))
