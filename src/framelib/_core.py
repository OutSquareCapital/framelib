from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyochain as pc

if TYPE_CHECKING:
    from ._database import Schema


def _default_schema() -> type[Schema]:
    from ._database import Schema

    return Schema


class BaseEntry(ABC):
    _name: str

    @property
    def name(self) -> str:
        """Get the name of the entry."""
        return self._name

    def __name_from_layout__(self, name: str) -> None:
        self._name = name


def _add_to_schema(name: str, obj: BaseEntry, schema: dict[str, Any]) -> None:
    obj.__name_from_layout__(name)
    schema[name] = obj


class BaseLayout[T](ABC):
    """A BaseLayout represents a static layout containing multiple entries.

    Each entry is of type T, which is typically a subclass of BaseEntry.
    The layout can be a Folder (containing File entries) or a Database (containing Table entries).

    It has a schema method that returns a dictionary of its entries.
    """

    _schema: dict[str, T]

    def __init_subclass__(cls) -> None:
        cls._schema: dict[str, T] = {}
        cls._add_entries()

    @classmethod
    def _add_entries(cls) -> pc.Dict[str, BaseEntry]:
        return (
            pc.Dict.from_object(cls)
            .filter_type(BaseEntry)
            .for_each(_add_to_schema, cls._schema)
        )

    @classmethod
    def schema(cls) -> pc.Dict[str, T]:
        """Gets the schema dictionary of the layout.

        Each value is an Entry instance.

        For example, for a `Folder` layout, the schema will contain `File` instances.

        For a `Database` layout, the schema will contain `Table` instances.

        Returns:
            pc.Dict[str, T]: the schema dictionary of the layout as a pychain.Dict
        """
        return pc.Dict(cls._schema)


class Entry(BaseEntry):
    """An `Entry` represents any class that can be instantiated and used as an attribute in a `Layout`.

    It has a `source` attribute representing its `Path` location and a `model` of `type[T]` attribute representing its schema or data model.

    Args:
        model (type[Schema] | None): The model type associated with the entry. Defaults to object.
    """

    _model: type[Schema]
    __source__: Path

    def __init__(self, model: type[Schema] | None = None) -> None:
        if model is None:
            model = _default_schema()
        self._model = model

    def __set_source__(self, source: Path) -> None:
        self.__source__ = source

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(\nsource={self.source},\nmodel={self._model}\n)"
        )

    @property
    def model(self) -> type[Schema]:
        """Get the model type associated with the entry."""
        return self._model

    @property
    def source(self) -> Path:
        """Get the source location of the entry."""
        return self.__source__
