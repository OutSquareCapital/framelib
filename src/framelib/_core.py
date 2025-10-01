from __future__ import annotations

from abc import ABC
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, Self

import pychain as pc

from ._tree import show_tree


class EntryType(StrEnum):
    ENTRY_TYPE = "_is_entry_type"
    FILE = "_is_file"
    TABLE = "_is_table"
    REQUEST = "_is_request"
    COLUMN = "_is_column"
    SOURCE = "__source__"


class PathLike(Protocol):
    def joinpath(self, *other: Any) -> Self: ...
    def with_suffix(self, suffix: str) -> Self: ...


class BaseEntry(ABC):
    _name: str

    @property
    def name(self) -> str:
        return self._name

    def __name_from_layout__(self, name: str) -> None:
        self._name = name


class BaseLayout[T: BaseEntry](ABC):
    __source__: Path
    _schema: dict[str, T]
    _is_entry_type: EntryType

    def __init_subclass__(cls) -> None:
        if not hasattr(cls, EntryType.ENTRY_TYPE):
            pass
        else:
            if not hasattr(cls, EntryType.SOURCE):
                cls.__source__ = Path()

            cls.__source__ = cls.__source__.joinpath(cls.__name__.lower())
            cls._schema: dict[str, T] = {}
            for name, obj in cls.__dict__.items():
                if getattr(obj, cls._is_entry_type, False) is True:
                    obj.__name_from_layout__(name)
                    cls._schema[name] = obj

    @classmethod
    def schema(cls) -> pc.Dict[str, T]:
        return pc.Dict(cls._schema)

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


class Entry[T, U](BaseEntry):
    model: type[T]
    source: U

    def __init__(self, model: type[T] = object) -> None:
        self.model = model

    def __from_source__(self, source: U) -> None:
        self._build_source(source)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(\nsource={self.source},\nmodel={self.model}\n)"
        )

    def _build_source(self, source: U) -> None:
        self.source = source

    def _display_(self) -> str:
        return self.__repr__()

    def _repr_html_(self) -> str:
        return self._display_()
