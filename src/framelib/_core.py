from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar, Self, TypeGuard

import pychain as pc


class SourceUser[T](ABC):
    source: Any
    schema: type[T]

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[SourceUser[Any]]: ...

    def __from_source__(self, source: Any, name: str) -> None: ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source={self.source})"

    def _display_(self) -> str:
        return self.__repr__()

    def _repr_html_(self) -> str:
        return self._display_()


class SourceSchema[T: SourceUser[Any]](ABC):
    __source__: ClassVar[Path]
    _member_type: type[T]
    _schema: ClassVar[dict[str, SourceUser[Any]]]

    def __init_subclass__(cls) -> None:
        cls._set_source()._set_schema()

    @classmethod
    @abstractmethod
    def _set_source(cls) -> type[Self]: ...
    @classmethod
    def _set_schema(cls) -> type[Self]:
        cls._schema = {}
        for name, obj in cls.__dict__.items():
            if cls._member_type.__identity__(obj):
                obj.__from_source__(cls.source(), name)
                cls._schema[name] = obj
            else:
                continue
        return cls

    @classmethod
    def source(cls) -> Path:
        return cls.__source__

    @classmethod
    def schema(cls) -> pc.Dict[str, SourceUser[Any]]:
        return pc.Dict(cls._schema)
