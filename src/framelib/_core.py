from __future__ import annotations

from abc import ABC
from enum import StrEnum
from typing import Any, Protocol, Self

import pychain as pc


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
    _schema: dict[str, T]
    _is_entry_type: EntryType

    def __init_subclass__(cls) -> None:
        cls._schema: dict[str, T] = {}
        for name, obj in cls.__dict__.items():
            if getattr(obj, cls._is_entry_type, False) is True:
                obj.__name_from_layout__(name)
                cls._schema[name] = obj

    @classmethod
    def schema(cls) -> pc.Dict[str, T]:
        return pc.Dict(cls._schema)


class Entry[T, U](BaseEntry):
    model: type[T]
    source: U

    def __init__(self, model: type[T] = object) -> None:
        self.model = model

    def __from_source__(self, source: U) -> None:
        self.source = source

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(\nsource={self.source},\nmodel={self.model}\n)"
        )
