from __future__ import annotations

from pathlib import Path
from typing import Any, TypeGuard

import pychain as pc

from ._tree import TreeDisplay


class FileReader:
    """
    File reader class for different file formats.

    The path attribute will be set automatically when used in a Folder subclass, using the variable name as the filename, and the subclass name as the file extension.

    if glob is set to True, the extension will be ignored, and the path will be generated without extension.

    This is primarily useful for partitioned parquet files.

    The read and scan properties returns partial functions from polars, with the path already set.
    """

    path: Path
    extension: str
    _is_file_reader = True
    __slots__ = ("path", "extension")

    def __init__(self, glob: bool = False) -> None:
        if glob:
            self.extension = ""
        else:
            self.extension = f".{self.__class__.__name__.lower()}"

    def from_dir(self, directory: Path | str, file_name: str) -> FileReader:
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
    def __identity__(obj: Any) -> TypeGuard[FileReader]:
        return getattr(obj, "_is_file_reader", False) is True

    def iter_dir(self) -> pc.Iter[Path]:
        """
        Returns a pychain iterator over the files in the directory containing this schema's file.
        """
        return pc.Iter(self.path.iterdir())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(path={self.path})"

    def _display_(self) -> TreeDisplay:
        return TreeDisplay(self.path)

    def _repr_html_(self) -> TreeDisplay:
        return self._display_()
