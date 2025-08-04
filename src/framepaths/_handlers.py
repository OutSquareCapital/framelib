from enum import StrEnum
from functools import partial
from pathlib import Path as _Path
from typing import Literal

import polars as pl
from dataframely import Schema as _Schema

from ._tree import TreeDisplay

Formatting = Literal["upper", "lower", "title"]


class Extension(StrEnum):
    CSV = ".csv"
    PARQUET = ".parquet"
    NDJSON = ".ndjson"


class Schema(_Schema):
    """
    Base schema for file-based schemas, providing path and tree display utilities.
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


class CSVSchema(Schema):
    __ext__ = Extension.CSV

    def read(self):
        return partial(pl.read_csv, source=self.path())

    def scan(self):
        return partial(pl.scan_csv, source=self.path())


class ParquetSchema(Schema):
    __ext__ = Extension.PARQUET

    def read(self):
        return partial(pl.read_parquet, source=self.path())

    def scan(self):
        return partial(pl.scan_parquet, source=self.path())


class NDJSONSchema(Schema):
    __ext__ = Extension.NDJSON

    def read(self):
        return partial(pl.read_ndjson, source=self.path())

    def scan(self):
        return partial(pl.scan_ndjson, source=self.path())
