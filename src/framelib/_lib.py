from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Any, TypeGuard

import polars as pl
import pychain as pc

from ._core import FileReader
from ._tree import TreeDisplay


class Parquet(FileReader):
    @property
    def scan(self):
        return partial(pl.scan_parquet, self.path)

    @property
    def read(self):
        return partial(pl.read_parquet, self.path)

    def read_schema(self) -> dict[str, pl.DataType]:
        """
        Get the schema of a Parquet file without reading data.

        If you would like to read the schema of a cloud file with authentication
        configuration, it is recommended use `scan_parquet` - e.g.
        `scan_parquet(..., storage_options=...).collect_schema()`.

        Parameters
        ----------
        source
            Path to a file or a file-like object (by "file-like object" we refer to objects
            that have a `read()` method, such as a file handler like the builtin `open`
            function, or a `BytesIO` instance). For file-like objects, the stream position
            may not be updated accordingly after reading.

        Returns
        -------
        dict
            Dictionary mapping column names to datatypes

        See Also
        --------
        scan_parquet
        """
        return pl.read_parquet_schema(self.path)

    def read_metadata(self) -> dict[str, str]:
        """
        Get file-level custom metadata of a Parquet file without reading data.

        .. warning::
            This functionality is considered **experimental**. It may be removed or
            changed at any point without it being considered a breaking change.

        Parameters
        ----------
        source
            Path to a file or a file-like object (by "file-like object" we refer to objects
            that have a `read()` method, such as a file handler like the builtin `open`
            function, or a `BytesIO` instance). For file-like objects, the stream position
            may not be updated accordingly after reading.

        Returns
        -------
        dict
            Dictionary with the metadata. Empty if no custom metadata is available.
        """
        return pl.read_parquet_metadata(self.path)


class CSV(FileReader):
    @property
    def scan(self):
        return partial(pl.scan_csv, self.path)

    @property
    def read(self):
        return partial(pl.read_csv, self.path)

    @property
    def read_batched(self):
        return partial(pl.read_csv_batched, self.path)


class NDJson(FileReader):
    @property
    def scan(self):
        return partial(pl.scan_ndjson, self.path)

    @property
    def read(self):
        return partial(pl.read_ndjson, self.path)


class Json(FileReader):
    @property
    def read(self):
        return partial(pl.read_json, self.path)


class Folder:
    """
    Folder schema class to organize FileReader instances, used as a base class.
    The __directory__ attribute will be set automatically when subclassed, using the subclass name as the folder name.

    The FileReader instances will have their path attribute set automatically, using the variable name as the filename, and the subclass name as the file extension.

    For example:
    >>> class MyDirectory(Folder):
    ...     __directory__ = Path("data")
    ...     users = CSV()
    ...     orders = Parquet()
    >>> MyDirectory.directory().as_posix()
    'data/mydirectory'
    >>> MyDirectory.users.path.as_posix()
    'data/mydirectory/users.csv'

    """

    __directory__: Path
    _is_folder = True

    def __init_subclass__(cls) -> None:
        if not hasattr(cls, "__directory__"):
            raise AttributeError(
                f"Class {cls.__name__} must define a __directory__ attribute"
            )
        cls.__directory__ = cls.__directory__.joinpath(cls.__name__.lower())
        for name, obj in cls.__dict__.items():
            if FileReader.__identity__(obj):
                obj.from_dir(cls.directory(), name)
            else:
                continue

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[Folder]:
        return getattr(obj, "_is_folder", False) is True

    @classmethod
    def show_tree(cls) -> TreeDisplay:
        """
        Returns a TreeDisplay object for this schema's directory.
        """
        return TreeDisplay(root=cls.__directory__)

    @classmethod
    def iter_dir(cls) -> pc.Iter[Path]:
        """
        Returns an iterator over the FileReader instances in the folder.
        """
        return pc.Iter(cls.__directory__.iterdir())

    @classmethod
    def directory(cls) -> Path:
        """
        Returns the directory path of this Folder subclass.
        """
        return cls.__directory__
