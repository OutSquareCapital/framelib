from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, Self

import dataframely as dy
import duckdb
import narwhals as nw
import polars as pl
import pychain as pc
from narwhals.typing import IntoFrame, IntoFrameT, IntoLazyFrame, IntoLazyFrameT

from . import _queries as qry
from ._core import BaseEntry, BaseLayout, Entry, EntryType
from ._schema import Schema
from ._tree import show_tree


class Table(Entry[Schema, Path]):
    _is_table: Final[bool] = True

    def _from_df(self, df: IntoFrameT | IntoLazyFrameT) -> IntoFrameT | IntoLazyFrameT:
        return nw.from_native(df).lazy().pipe(self.model.cast).to_native()

    def with_connexion(self, con: duckdb.DuckDBPyConnection) -> Self:
        self._con = con
        return self

    def read(self) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
        return nw.from_native(self._con.table(self._name))

    def create_or_replace_from(self, df: IntoFrame | IntoLazyFrame) -> Self:
        _ = self._from_df(df)
        self._con.execute(qry.create_or_replace(self._name))
        pk_names: list[str] = self.model.primary_keys().pipe_unwrap(list)
        if pk_names:
            self._con.execute(qry.add_primary_key(self._name, *pk_names))
        return self

    def append(self, df: IntoFrame | IntoLazyFrame) -> Self:
        _ = self._from_df(df)
        self._con.execute(qry.insert_into(self._name))
        return self

    def create_from(self, df: IntoFrame | IntoLazyFrame) -> Self:
        _ = self._from_df(df)
        self._con.execute(qry.create_from(self._name))
        return self

    def truncate(self) -> Self:
        self._con.execute(qry.truncate(self._name))
        return self

    def drop(self) -> Self:
        self._con.execute(qry.drop(self._name))
        return self

    def insert_if_not_exists(self, df: IntoFrame | IntoLazyFrame) -> Self:
        _ = self._from_df(df)
        keys: list[str] = self.model.primary_keys().pipe_unwrap(list)
        if not keys:
            raise ValueError(
                f"Cannot perform 'insert_if_not_exists' on table '{self._name}' "
                "because no primary keys are defined in its schema."
            )
        self._con.execute(qry.insert_if_not_exists(self._name, *keys))
        return self


class DataBase(BaseLayout[Table], BaseEntry, ABC):
    _is_file: Final[bool] = True
    _connexion: duckdb.DuckDBPyConnection
    _is_entry_type = EntryType.TABLE
    source: Path
    model: pc.Dict[str, Table]

    def __from_source__(self, source: Path) -> None:
        self.source = Path(source, self._name).with_suffix(".ddb")
        self.model = self.schema()
        for table in self._schema.values():
            table.source = self.source

    def __enter__(self) -> Self:
        self._connexion = duckdb.connect(self.source)
        for table in self._schema.values():
            table.with_connexion(self._connexion)
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._connexion.close()

    @property
    def connexion(self) -> duckdb.DuckDBPyConnection:
        return self._connexion


class File[T: dy.Schema](Entry[T, Path]):
    _is_file: Final[bool] = True
    _with_suffix: bool = True

    def __from_source__(self, source: Path | str) -> None:
        self.source = Path(source, self._name)
        if self.__class__._with_suffix:
            self.source = self.source.with_suffix(f".{self.__class__.__name__.lower()}")

    @property
    @abstractmethod
    def read(self) -> Callable[..., pl.DataFrame]:
        raise NotImplementedError

    @property
    def scan(self) -> Callable[..., pl.LazyFrame]:
        raise NotImplementedError

    @property
    @abstractmethod
    def write(self) -> Any:
        raise NotImplementedError

    def read_cast(self) -> pl.DataFrame:
        """
        Read the file and cast it to the defined schema.
        """
        return self.read().pipe(self.model.cast)

    def scan_cast(self) -> pl.LazyFrame:
        """
        Scan the file and cast it to the defined schema.
        """
        return self.scan().pipe(self.model.cast)

    def write_cast(
        self, df: pl.LazyFrame | pl.DataFrame, *args: Any, **kwargs: Any
    ) -> None:
        """
        Cast the dataframe to the defined schema and write it to the file.
        """
        self.model.cast(df.lazy().collect()).pipe(self.write, *args, **kwargs)


class Folder(BaseLayout[File[dy.Schema]]):
    _is_entry_type = EntryType.FILE

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if not hasattr(cls, EntryType.SOURCE):
            cls.__source__ = Path()

        cls.__source__ = cls.__source__.joinpath(cls.__name__.lower())
        for file in cls._schema.values():
            file.__from_source__(cls.source())

    @classmethod
    def source(cls) -> Path:
        return cls.__source__

    @classmethod
    def show_tree(cls) -> str:
        return show_tree(cls.__source__)

    @classmethod
    def _display_(cls) -> str:
        return cls.show_tree()

    @classmethod
    def iter_dir(cls) -> pc.Iter[Path]:
        """
        Returns an iterator over the File instances in the folder.
        """
        return pc.Iter(cls.__source__.iterdir())

    @classmethod
    def clean(cls) -> type[Self]:
        """
        Delete all files in the folder and recreate the directory.
        """
        import shutil

        source_path: Path = cls.source()
        if source_path.exists():
            shutil.rmtree(source_path)
        return cls
