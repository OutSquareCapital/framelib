from __future__ import annotations

from pathlib import Path
from typing import Any, Self

import dataframely as dy
import duckdb
import pychain as pc

from .._core import BaseLayout, EntryType
from .._schema import Schema
from .._tree import show_tree
from ._entries import Entry, File, Request, Table


class Layout[T: Entry[Any, Any]](BaseLayout[T]):
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


class DataBase(Layout[Table[Schema]]):
    _con: duckdb.DuckDBPyConnection
    _is_entry_type = EntryType.TABLE

    @classmethod
    def __enter__(cls) -> type[Self]:
        cls._con = duckdb.connect(cls.source())
        for table in cls._schema.values():
            table.with_connexion(cls._con)
        return cls

    @classmethod
    def __exit__(cls, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        cls._con.close()

    @classmethod
    def __del__(cls) -> None:
        cls._con.close()

    @classmethod
    def connexion(cls) -> duckdb.DuckDBPyConnection:
        return cls._con


class Folder(Layout[File[dy.Schema]]):
    _is_entry_type = EntryType.FILE

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        for file in cls._schema.values():
            file._build_source(cls.source())  # type: ignore


class Server(Layout[Request[Any]]):
    _is_entry_type = EntryType.REQUEST
