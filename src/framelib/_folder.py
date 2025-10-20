from __future__ import annotations

from pathlib import Path
from typing import Self

import pychain as pc

from ._core import BaseLayout, EntryType
from ._filehandlers import File
from ._schema import Schema
from ._tree import show_tree


class Folder(BaseLayout[File[Schema]]):
    __entry_type__ = EntryType.FILE

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if not hasattr(cls, EntryType.SOURCE):
            cls.__source__ = Path()

        cls.__source__ = cls.__source__.joinpath(cls.__name__.lower())
        cls.schema().iter_values().for_each(lambda f: f.__from_source__(cls.source()))

    @classmethod
    def source(cls) -> Path:
        """Returns the source path of the folder."""
        return cls.__source__

    @classmethod
    def show_tree(cls) -> str:
        """Show the folder structure."""
        return show_tree(cls.mro())

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
