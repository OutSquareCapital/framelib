from __future__ import annotations

from pathlib import Path
from typing import Any, Final, Self, TypeGuard

import duckdb
import pychain as pc

from .._core import SourceSchema
from .._schema import Schema
from .._tree import show_tree


class DataBase(SourceSchema[Table[Schema]]):
    _is_db: Final[bool] = True
    _con: duckdb.DuckDBPyConnection
    __slots__ = ("_con",)

    def __init__(self) -> None:
        self._set_connexion()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self._con.close()

    def __del__(self) -> None:
        self._con.close()

    @classmethod
    def _set_source(cls) -> type[Self]:
        if not hasattr(cls, "__source__"):
            cls.__source__ = Path()
        cls.__source__ = cls.source().joinpath(cls.__name__.lower()).with_suffix(".db")
        return cls

    def _set_connexion(self) -> Self:
        self._con = duckdb.connect(self.source())
        for table in self._schema.values():
            table.with_connexion(self._con)
        return self

    @property
    def connexion(self) -> duckdb.DuckDBPyConnection:
        return self._con


class Folder(SourceSchema[FileReader[Any]]):
    """
    Folder schema class to organize FileReader instances, used as a base class.

    If not provided, the __directory__ attribute will be set automatically when subclassed as `Path()`.

    Then, the subclass name will be used as a subdirectory.

    The FileReader instances will have their path attribute set automatically, using the variable name as the filename, and the subclass name as the file extension.

    For example:
    >>> from pathlib import Path
    >>> from framelib import Folder, CSV, Parquet
    >>> class MyDirectory(Folder):
    ...     __directory__ = Path("data")
    ...     users = CSV()
    ...     orders = Parquet()
    >>> MyDirectory.directory().as_posix()
    'data/mydirectory'
    >>> MyDirectory.users.path.as_posix()
    'data/mydirectory/users.csv'

    """

    _is_folder: Final[bool] = True

    @classmethod
    def _set_source(cls) -> type[Self]:
        if not hasattr(cls, "__source__"):
            cls.__source__ = Path()
        cls.__source__ = cls.source().joinpath(cls.__name__.lower())
        return cls

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[Folder]:
        return getattr(obj, "_is_folder", False) is True

    @classmethod
    def show_tree(cls) -> str:
        return show_tree(cls.__source__)

    @classmethod
    def _display_(cls) -> str:
        return cls.show_tree()

    @classmethod
    def iter_dir(cls) -> pc.Iter[Path]:
        """
        Returns an iterator over the FileReader instances in the folder.
        """
        return pc.Iter(cls.__source__.iterdir())
