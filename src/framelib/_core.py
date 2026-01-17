from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, TypeIs

import pyochain as pc

if TYPE_CHECKING:
    from ._database import Schema


def _default_schema() -> type[Schema]:
    from ._database import Schema

    return Schema


class BaseEntry(ABC):
    """A `BaseEntry` represents any class that can be instantiated and used as an attribute in a `Layout`.

    It has a `name` attribute representing its name in the layout.
    """

    _name: str

    @property
    def name(self) -> str:
        """Get the name of the entry."""
        return self._name

    def __name_from_layout__(self, name: str) -> None:
        self._name = name


class Layout[T: BaseEntry](ABC):
    r"""A `Layout` represents a static layout containing multiple `BaseEntry` instances.

    Each entry is of type T, which is typically a subclass of `BaseEntry`.

    The layout can be a `Folder` (containing `File` entries) or a `Database` (containing `Table` entries).
    """

    _schema: pc.Dict[str, T]

    def __init_subclass__(cls) -> None:
        def _is_base_entry(obj: object) -> TypeIs[T]:
            return isinstance(obj, BaseEntry)

        cls._schema = (
            pc.Iter(cls.__dict__.items())  # TODO: typeIs for star functions
            .filter_star(lambda _, obj: _is_base_entry(obj))
            .collect(pc.Dict)
            .inspect(
                lambda x: x.items()
                .iter()
                .for_each_star(lambda name, entry: entry.__name_from_layout__(name))
            )
        )

    @classmethod
    def schema(cls) -> pc.Dict[str, T]:
        """Gets the schema dictionary of the layout.

        Each value is an Entry instance corresponding to the attribute in the layout.

        Returns:
            pc.Dict[str, T]: the schema dictionary of the layout as a pyochain.Dict
        """
        return cls._schema


class Entry(BaseEntry):
    r"""An `Entry` represents any class that can be instantiated and used as an attribute in a `Layout`.

    It has a `source` attribute representing its `Path` location,
    and a `model` of `type[T]` attribute representing its schema or data model.

    Args:
        model (type[Schema] | None): The model type associated with the entry. Defaults to object.
    """

    _model: type[Schema]
    __source__: Path

    def __init__(self, model: type[Schema] | None = None) -> None:
        self._model = _default_schema() if model is None else model

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
