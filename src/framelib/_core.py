from __future__ import annotations

from pathlib import Path
from typing import Any, TypeGuard

import pychain as pc


class FileReader:
    """
    File reader class for different file formats.

    The path attribute will be set automatically when used in a Folder subclass, using the variable name as the filename, and the subclass name as the file extension.
    For example:

    >>> class MyData(Folder):
    ...     __directory__ = Path("data")
    ...     users = CSV()
    ...     orders = Parquet()
    >>> MyData.users.path.as_posix()
    'data/users.csv'

    The read and scan properties returns partial functions from polars, with the path already set.
    """

    path: Path
    _is_file_reader = True
    __slots__ = ("path",)

    def from_dir(self, directory: Path | str, file_name: str) -> FileReader:
        """
        Set the path attribute for this file reader instance from a directory and file name.

        Used internally by the Folder initialization process, or can be used manually as a convenience method.
        >>> CSV().from_dir("my_folder", "my_data").path.as_posix()
        'my_folder/my_data.csv'
        """
        self.path = (
            Path(directory).joinpath(file_name).with_suffix(f".{self.extension}")
        )
        return self

    @property
    def extension(self) -> str:
        """
        Returns the file extension for this file reader, defined from the class name in lowercase.
        >>> CSV().extension
        'csv'
        """
        return self.__class__.__name__.lower()

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[FileReader]:
        return getattr(obj, "_is_file_reader", False) is True

    def iter_dir(self) -> pc.Iter[Path]:
        """
        Returns a pychain iterator over the files in the directory containing this schema's file.
        """
        return pc.Iter(self.path.iterdir())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(path={self.path})"
