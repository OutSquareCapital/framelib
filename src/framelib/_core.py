from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any, Self, TypeGuard

import dataframely as dy
import polars as pl
import pychain as pc

from ._tree import TreeDisplay


class FileReader[T: dy.Schema](ABC):
    """
    File reader class for different file formats.

    The path attribute will be set automatically when used in a Folder subclass, using the variable name as the filename, and the subclass name as the file extension.

    if glob is set to True, the extension will be ignored, and the path will be generated without extension.

    This is primarily useful for partitioned parquet files.

    The read and scan properties returns partial functions from polars, with the path already set.
    """

    path: Path
    extension: str
    schema: type[T]
    _is_file_reader = True
    __slots__ = ("path", "extension", "schema")

    def __init__(self, schema: type[T] = dy.Schema) -> None:
        self.extension = f".{self.__class__.__name__.lower()}"
        self.schema: type[T] = schema

    def from_dir(self, directory: Path | str, file_name: str) -> Self:
        """
        Set the path attribute for this file reader instance from a directory and file name.

        Used internally by the Folder initialization process, or can be used manually as a convenience method.

        >>> from framelib import CSV
        >>> CSV().from_dir("my_folder", "my_data").path.as_posix()
        'my_folder/my_data.csv'
        """
        self.path = Path(directory, file_name).with_suffix(self.extension)
        return self

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[FileReader[Any]]:
        return getattr(obj, "_is_file_reader", False) is True

    def iter_dir(self) -> pc.Iter[Path]:
        """
        Returns a pychain iterator over the files in the directory containing this schema's file.
        """
        return pc.Iter(self.path.iterdir())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(path={self.path})"

    def _display_(self) -> str:
        return TreeDisplay(self.path)._display_()  # type: ignore

    def _repr_html_(self) -> str:
        return self._display_()

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

    def read_cast(self) -> dy.DataFrame[T]:
        """
        Read the file and cast it to the defined schema.
        """
        return self.read().pipe(self.schema.cast)

    def scan_cast(self) -> dy.LazyFrame[T]:
        """
        Scan the file and cast it to the defined schema.
        """
        return self.scan().pipe(self.schema.cast)

    def write_cast(
        self, df: pl.LazyFrame | pl.DataFrame, *args: Any, **kwargs: Any
    ) -> None:
        """
        Cast the dataframe to the defined schema and write it to the file.
        """
        self.schema.cast(df.lazy().collect()).pipe(self.write, *args, **kwargs)


class Folder:
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

    __directory__: Path
    _is_folder = True

    def __init_subclass__(cls) -> None:
        cls._set_directory()._set_schema()

    @classmethod
    def _set_directory(cls) -> type[Self]:
        if not hasattr(cls, "__directory__"):
            cls.__directory__ = Path()
        cls.__directory__ = cls.directory().joinpath(cls.__name__.lower())
        return cls

    @classmethod
    def _set_schema(cls) -> type[Self]:
        for name, obj in cls.__dict__.items():
            if FileReader.__identity__(obj):
                obj.from_dir(cls.directory(), name)
            else:
                continue
        return cls

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[Folder]:
        return getattr(obj, "_is_folder", False) is True

    @classmethod
    def show_tree(cls) -> TreeDisplay:
        return TreeDisplay(cls.__directory__)

    @classmethod
    def _display_(cls) -> TreeDisplay:
        return cls.show_tree()

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
