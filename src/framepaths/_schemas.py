import functools
from abc import ABC
from collections.abc import Callable
from pathlib import Path as _Path
from typing import Concatenate, overload

from dataframely import Schema as _Schema

from ._tree import TreeDisplay
from ._types import Formatting


class Schema(_Schema, ABC):
    """
    Base schema for file-based schemas, providing path and tree display utilities.

    Subclasses should define the `__ext__` attribute, as well as implement the `read` and `scan` methods.
    """

    __directory__: str | _Path
    __ext__: str

    @classmethod
    def path(cls, make_dir: bool = False, format: Formatting | None = None) -> _Path:
        """
        Returns the full path to the file for this schema, with optional directory creation and name formatting.
        """
        name = cls.__name__
        match format:
            case "upper":
                name = name.upper()
            case "lower":
                name = name.lower()
            case "title":
                name = name.title()
            case _:
                pass
        if not cls.__directory__:
            raise ValueError("Schema must have a __directory__ attribute set.")
        if not cls.__ext__:
            raise ValueError("Schema must have a __ext__ attribute set.")
        path = _Path(cls.__directory__).joinpath(f"{name}{cls.__ext__}")
        if make_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def show_tree(cls) -> TreeDisplay:
        """
        Returns a TreeDisplay object for the directory containing this schema's file.
        """
        root_dir = _Path(cls.path().parent)
        return TreeDisplay(root=root_dir, title=cls.__name__)


class IODescriptor[**P, T]:
    __slots__ = ("func", "public_name")

    def __init__(self, func: Callable[Concatenate[_Path, P], T]) -> None:
        self.func = func
        self.public_name: str | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        self.public_name = name
        functools.update_wrapper(self.__class__, self.func)

    @overload
    def __get__(self, instance: None, owner: type[Schema]) -> Callable[P, T]: ...

    @overload
    def __get__(self, instance: Schema, owner: type[Schema]) -> Callable[P, T]: ...

    def __get__(self, instance: Schema | None, owner: type[Schema]) -> Callable[P, T]:
        return functools.partial(self.func, owner.path())
