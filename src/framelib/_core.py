from __future__ import annotations

from abc import ABC
from enum import StrEnum
from typing import Any, Protocol, Self

import pyochain as pc


class EntryType(StrEnum):
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
        """Returns the name of the entry as defined in the layout."""
        return self._name

    def __name_from_layout__(self, name: str) -> None:
        self._name = name


def _add_to_schema(name: str, obj: BaseEntry, schema: dict[str, Any]) -> None:
    obj.__name_from_layout__(name)
    schema[name] = obj


class BaseLayout[T](ABC):
    _schema: dict[str, T]
    __entry_type__: EntryType

    def __init_subclass__(cls) -> None:
        cls._schema: dict[str, T] = {}
        pc.Dict.from_object(cls).filter_attr(cls.__entry_type__, BaseEntry).for_each(
            _add_to_schema, cls._schema
        )

    @classmethod
    def schema(cls) -> pc.Dict[str, T]:
        """Returns the schema dictionary of the layout as a pychain.Dict"""
        return pc.Dict(cls._schema)


class Entry[T, U](BaseEntry):
    model: type[T]
    source: U

    def __init__(self, model: type[T] = object) -> None:
        self.model = model

    def __from_source__(self, source: U) -> None:
        self.source = source

    def __repr__(self) -> str:
        return f"{self._cls_name}(\nsource={self.source},\nmodel={self.model}\n)"

    @property
    def _cls_name(self) -> str:
        return self.__class__.__name__
