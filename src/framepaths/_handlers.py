from abc import ABC, abstractmethod
from functools import partial
from pathlib import Path as _Path

import polars as pl
from dataframely import Schema as _Schema

from ._tree import TreeDisplay
from ._types import Extension, Formatting


class Schema(_Schema, ABC):
    """
    Base schema for file-based schemas, providing path and tree display utilities.

    Subclasses should define the `__ext__` attribute, as well as implement the `read` and `scan` methods.
    """

    __directory__: str | _Path
    __ext__: str | Extension

    @classmethod
    def path(cls, make_dir: bool = False, format: Formatting | None = None) -> _Path:
        """
        Returns the full path to the file for this schema, with optional directory creation and name formatting.
        """
        name = cls.__name__
        match format:
            case "upper":
                name = name.upper()
            case "lower":
                name = name.lower()
            case "title":
                name = name.title()
            case _:
                pass
        if not cls.__directory__:
            raise ValueError("Schema must have a __directory__ attribute set.")
        if not cls.__ext__:
            raise ValueError("Schema must have a __ext__ attribute set.")
        path = _Path(cls.__directory__).joinpath(f"{name}{cls.__ext__}")
        if make_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def show_tree(cls) -> TreeDisplay:
        """
        Returns a TreeDisplay object for the directory containing this schema's file.
        """
        root_dir = _Path(cls.path().parent)
        return TreeDisplay(root=root_dir, title=cls.__name__)

    @classmethod
    @abstractmethod
    def read(cls) -> partial[pl.DataFrame]:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def scan(cls) -> partial[pl.LazyFrame]:
        raise NotImplementedError


class CSVSchema(Schema):
    __ext__ = Extension.CSV

    @classmethod
    def read(cls):
        return partial(pl.read_csv, source=cls.path())

    @classmethod
    def scan(cls):
        return partial(pl.scan_csv, source=cls.path())


class ParquetSchema(Schema):
    __ext__ = Extension.PARQUET

    @classmethod
    def read(cls):
        return partial(pl.read_parquet, source=cls.path())

    @classmethod
    def scan(cls):
        return partial(pl.scan_parquet, source=cls.path())


class NDJSONSchema(Schema):
    __ext__ = Extension.NDJSON

    @classmethod
    def read(cls):
        return partial(pl.read_ndjson, source=cls.path())

    @classmethod
    def scan(cls):
        return partial(pl.scan_ndjson, source=cls.path())
