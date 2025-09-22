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
        >>> from pathlib import Path
        >>> class MyFile(Schema):
        ...     __directory__ = Path("tests").joinpath("data_schema")
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
    def path(cls, make_dir: bool = False) -> pathlib.Path:
        """
        Returns the full path to the file for this schema.
        """
        if not cls.__ext__:
            raise ValueError("Schema must have a __ext__ attribute set.")
        path = cls.__directory__.joinpath(cls.__name__).with_suffix(cls.__ext__)
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
        """
        Returns an iterator over the column names in the schema.
        """
        return pc.Iter(cls.column_names())

    @classmethod
    def iter_exprs(cls) -> pc.Iter[pl.Expr]:
        """
        Returns an iterator over the expression representations of the columns in the schema.
        """
        return cls.iter_columns().map(lambda expr: expr.col)

    @classmethod
    def iter_columns(cls) -> pc.Iter[dy.Column]:
        """
        Returns an iterator over the columns in the schema.
        """
        return pc.Iter(cls.columns().values())

    @classmethod
    def to_dict(cls) -> pc.Dict[str, dy.Column]:
        """
        Returns a dictionary representation of the schema, wrapped in a pychain Dict instance.
        """
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
