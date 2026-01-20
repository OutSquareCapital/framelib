from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, TypeIs

import pyochain as pc

if TYPE_CHECKING:
    from ._schema import Schema


def _default_schema() -> type[Schema]:
    from ._schema import Schema

    return Schema


class BaseEntry(ABC):
    """A `BaseEntry` represents any class that can be instantiated and used as an attribute in a `Layout`.

    It has a `name` attribute representing its name in the layout.
    """

    _name: str
    __slots__ = ("_name",)

    @property
    def name(self) -> str:
        """Get the name of the entry."""
        return self._name

    def __set_entry_name__(self, name: str) -> None:
        self._name = name


class Layout[T: BaseEntry](ABC):
    r"""A `Layout` represents a static layout containing multiple `BaseEntry` instances.

    Each entry is of type T, which is typically a subclass of `BaseEntry`.

    The layout can be a `Folder` (containing `File` entries) or a `Database` (containing `Table` entries).
    """

    _entries: pc.Dict[str, T]

    def __init_subclass__(cls) -> None:
        def _is_base_entry(obj: object) -> TypeIs[T]:
            return isinstance(obj, BaseEntry)

        cls._entries = (
            pc.Vec.from_ref(cls.mro())
            .rev()
            .filter(lambda c: c is not object and hasattr(c, "__dict__"))
            .flat_map(lambda c: c.__dict__.items())
            .filter_star(lambda _, obj: _is_base_entry(obj))
            .collect(pc.Dict)
            .inspect(
                lambda x: x.items()
                .iter()
                .for_each_star(lambda name, entry: entry.__set_entry_name__(name))
            )
        )

    @classmethod
    def entries(cls) -> pc.Dict[str, T]:
        """Gets the entries dictionary of the layout.

        Each value is an Entry instance corresponding to the attribute in the layout.

        Returns:
            pc.Dict[str, T]: the entries dictionary of the layout as a pyochain.Dict
        """
        return cls._entries


class Entry(BaseEntry):
    r"""An `Entry` represents any class that can be instantiated and used as an attribute in a `Layout`.

    It has a `source` attribute representing its `Path` location,
    and a `schema` of `type[T]` attribute representing its schema or data schema.

    Args:
        schema (type[Schema] | None): The schema type associated with the entry. Defaults to object.
    """

    _schema: type[Schema]
    __source__: Path
    __slots__ = ("__source__", "_schema")

    def __init__(self, schema: type[Schema] | None = None) -> None:
        self._schema = _default_schema() if schema is None else schema

    def __set_source__(self, source: Path) -> None:
        self.__source__ = source

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(\nsource={self.source},\nschema={self._schema}\n)"

    @property
    def schema(self) -> type[Schema]:
        """Get the schema associated with the entry."""
        return self._schema

    @property
    def source(self) -> Path:
        """Get the source location of the entry."""
        return self.__source__
