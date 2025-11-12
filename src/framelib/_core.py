from __future__ import annotations

from abc import ABC
from enum import StrEnum
from typing import Any

import pyochain as pc


class EntryType(StrEnum):
    FILE = "_is_file"
    TABLE = "_is_table"
    REQUEST = "_is_request"
    COLUMN = "_is_column"
    SOURCE = "__source__"


class BaseEntry(ABC):
    _name: str

    @property
    def name(self) -> str:
        """
        Returns:
            str: the name of the entry as defined in the layout.
        """
        return self._name

    def __name_from_layout__(self, name: str) -> None:
        self._name = name


def _add_to_schema(name: str, obj: BaseEntry, schema: dict[str, Any]) -> None:
    obj.__name_from_layout__(name)
    schema[name] = obj


class BaseLayout[T](ABC):
    """
    A BaseLayout represents a static layout containing multiple entries.
    Each entry is of type T, which is typically a subclass of BaseEntry.
    The layout can be a Folder (containing File entries) or a Database (containing Table entries).

    It has a schema method that returns a dictionary of its entries.
    """

    _schema: dict[str, T]
    __entry_type__: EntryType

    def __init_subclass__(cls) -> None:
        cls._schema: dict[str, T] = {}
        cls._add_entries()

    @classmethod
    def _add_entries(cls):
        return (
            pc.Dict.from_object(cls)
            .filter_attr(cls.__entry_type__, BaseEntry)
            .for_each(_add_to_schema, cls._schema)
        )

    @classmethod
    def schema(cls) -> pc.Dict[str, T]:
        """
        Gets the schema dictionary of the layout.

        Each value is an Entry instance.

        For example, for a `Folder` layout, the schema will contain `File` instances.

        For a `Database` layout, the schema will contain `Table` instances.
        Returns:
            out (Dict[str, T]): the schema dictionary of the layout as a pychain.Dict
        """
        return pc.Dict(cls._schema)


class Entry[T, U](BaseEntry):
    """
    An `Entry` represents any class that can be instantiated and used as an attribute in a `Layout`.

    It has a `source` attribute representing its location (Path, str, etc.) and a `model` attribute representing its schema or data model.
    """

    model: type[T]
    source: U

    def __init__(self, model: type[T] = object) -> None:
        """Initializes the Entry with an optional model type.
        Args:
            model (type[T], optional): The model type associated with the entry. Defaults to object.
        """
        self.model = model

    def __set_source__(self, source: U) -> None:
        self.source = source

    def __repr__(self) -> str:
        return f"{self._cls_name}(\nsource={self.source},\nmodel={self.model}\n)"

    @property
    def _cls_name(self) -> str:
        return self.__class__.__name__
