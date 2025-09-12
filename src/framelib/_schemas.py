import functools
import pathlib
from collections.abc import Callable
from typing import Concatenate, overload

import dataframely as dy
import polars as pl
import pychain as pc

from ._tree import TreeDisplay


class Schema(dy.Schema):
    """
    Base schema for file-based schemas, providing path and tree display utilities.

    Example:
        >>> @directory("tests", "data_schema")
        ... class MyFile(Schema):
        ...     __ext__ = ".dat"
        ...
        >>> MyFile.path().touch()
        >>> MyFile.show_tree()
        tests\\data_schema
        └── MyFile.dat

    Subclasses should define the `__ext__` attribute, as well as implement the `read` and `scan` methods.
    """

    __directory__: pathlib.Path
    __ext__: str

    @classmethod
    def path(cls, make_dir: bool = True) -> pathlib.Path:
        """
        Returns the full path to the file for this schema, with optional directory creation and name formatting.
        """
        if not cls.__ext__:
            raise ValueError("Schema must have a __ext__ attribute set.")
        path = cls.__directory__.joinpath(f"{cls.__name__}{cls.__ext__}")
        if make_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def show_tree(cls) -> TreeDisplay:
        """
        Returns a TreeDisplay object for the directory containing this schema's file.
        """
        return TreeDisplay(root=cls.path().parent)

    @classmethod
    def iter_names(cls) -> pc.Iter[str]:
        return pc.Iter(cls.column_names())

    @classmethod
    def iter_exprs(cls) -> pc.Iter[pl.Expr]:
        return cls.iter_columns().map(lambda expr: expr.col)

    @classmethod
    def iter_columns(cls) -> pc.Iter[dy.Column]:
        return pc.Iter(cls.columns().values())

    @classmethod
    def map_columns(cls) -> pc.Dict[str, dy.Column]:
        return pc.Dict(cls.columns())


class IODescriptor[**P, T]:
    def __init__(self, func: Callable[Concatenate[pathlib.Path, P], T]) -> None:
        self.func = func
        functools.update_wrapper(self, self.func)  # type: ignore[call-arg]

    @overload
    def __get__(self, instance: None, owner: type[Schema]) -> Callable[P, T]: ...

    @overload
    def __get__(self, instance: Schema, owner: type[Schema]) -> Callable[P, T]: ...

    def __get__(self, instance: Schema | None, owner: type[Schema]) -> Callable[P, T]:
        return functools.partial(self.func, owner.path())


def directory(*parts: str | pathlib.Path):
    """
    Decorator to set __directory__ on a Schema subclass, with a relative path.
    If parts are provided, adds them to the path (e.g. Path("tests", "data_schema")).
    """

    def _set_dir(cls: type[Schema]) -> type[Schema]:
        if parts:
            cls.__directory__ = pathlib.Path(*parts)
        else:
            cls.__directory__ = pathlib.Path()
        return cls

    return _set_dir
