from __future__ import annotations

from pathlib import Path
from typing import Self

import pyochain as pc

from ._core import BaseLayout, EntryType
from ._database import Schema
from ._filehandlers import File
from ._tree import show_tree


class Folder(BaseLayout[File[Schema]]):
    """
    A Folder represents a directory containing files.
    It's a `Schema` of `File` entries.

    """

    __entry_type__ = EntryType.FILE

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if not hasattr(cls, EntryType.SOURCE):
            cls.__source__ = Path()

        cls.__source__ = cls.__source__.joinpath(cls.__name__.lower())
        cls._set_files_source()

    @classmethod
    def _set_files_source(cls) -> None:
        return (
            cls.schema()
            .iter_values()
            .for_each(lambda file: file.__set_source__(cls.source()))
        )

    @classmethod
    def source(cls) -> Path:
        """
        Returns:
            Path: the source path of the folder.
        """
        return cls.__source__

    @classmethod
    def show_tree(cls) -> str:
        """
        Show the folder structure as a tree.

        Returns:
            str: The folder structure.

        Example:
        ```python
        >>> import framelib as fl
        >>> class MyFolder(fl.Folder):
        ...     data = fl.CsvFile()
        ...     logs = fl.JsonFile()
        >>> print(MyFolder.show_tree())
        MyFolder
        ├── data.csv
        └── logs.json

        ```
        """
        return show_tree(cls.mro())

    @classmethod
    def _display_(cls) -> str:
        return cls.show_tree()

    @classmethod
    def iter_dir(cls) -> pc.Iter[Path]:
        """
        Returns:
            out (pyochain.Iter[Path]): an iterator over the File instances in the folder.
        """
        return pc.Iter(cls.__source__.iterdir())

    @classmethod
    def clean(cls) -> type[Self]:
        """
        Delete all files in the folder and recreate the directory.
        Returns:
            type[Self]: The Folder class.
        """
        import shutil

        source_path: Path = cls.source()
        if source_path.exists():
            shutil.rmtree(source_path)
        return cls
