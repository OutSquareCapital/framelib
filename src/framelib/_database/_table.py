from pathlib import Path
from typing import Final, Self

import duckdb
import narwhals as nw
import polars as pl
import pychain as pc
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

    def _on_conflict(self) -> str:
        pks: list[str] = self.model.primary_keys().into(list)
        uks: pc.Seq[str] = self.model.unique_keys().collect()

        has_pk = bool(pks)
        needs_explicit_conflict: bool = (
            has_pk and bool(uks.unwrap())
        ) or uks.count() > 1

        if not needs_explicit_conflict:
            return self._qry.insert_or_replace()
        else:
            return self.model.column_names().pipe(
                self._qry.insert_on_conflict_update, pks if has_pk else [uks.first()]
            )

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
        primary key conflict.
        """
        _ = self._from_df(df)
        self._con.execute(self._on_conflict())
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

    def describe_columns(self) -> DuckFrame:
        """
        Returns detailed information about the columns of this table from the INFORMATION_SCHEMA.
        """
        return nw.from_native(self._con.sql(self._qry.columns_schema())).select(
            "column_name", "data_type", "is_nullable", "column_default"
        )

    def describe_constraints(self) -> DuckFrame:
        """
        Returns the constraints (PRIMARY KEY, UNIQUE) applied to this table.
        """
        return nw.from_native(self._con.sql(self._qry.constraints()))
