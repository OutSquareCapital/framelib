from collections.abc import Iterable
from enum import StrEnum
from typing import Literal, Self

import polars as pl
import pychain as pc

Kind = Literal["name", "value"]


class Enum(StrEnum):
    @classmethod
    def from_df(cls, name: str, data: pl.DataFrame | pl.LazyFrame) -> Self:
        """Create a dynamic Enum from values present in a DataFrame column.

        Example:
            >>> import polars as pl
            >>> df = pl.DataFrame({"col": ["b", "a", "b", "c"]})
            >>> Enum.from_df("col", df).to_list()
            ['a', 'b', 'c']
        """
        return cls(
            name,
            data.lazy()
            .select(pl.col(name).unique().sort())
            .collect()
            .get_column(name)
            .to_list(),
        )

    @classmethod
    def from_series(cls, data: pl.Series):
        """Create a dynamic Enum from a Series.

        Example:
            >>> Enum.from_series(pl.Series(["a", "a", "b", "c"])).to_list()
            ['a', 'b', 'c']
        """
        return cls(data.name, data.unique().sort().to_list())

    @classmethod
    def from_iter(cls, name: str, data: Iterable[str]) -> Self:
        """Create a dynamic Enum from an iterable of strings.

        Example:
            >>> Enum.from_iter(data=["a", 'a', "b", "c"], name="foo").to_list()
            ['a', 'b', 'c']
        """
        return cls(name, pc.Iter(data).unique().sort().pipe_into(list))

    @classmethod
    def to_series(cls, kind: Kind = "value") -> pl.Series:
        """Convert the Enum members to a Polars Series.

        Example:
            >>> class MyEnum(Enum):
            ...     value1 = "value1"
            ...     value2 = "value2"
            ...     value3 = "value3"
            >>> MyEnum.to_series().to_list()
            ['value1', 'value2', 'value3']
        """
        values = cls.to_iter(kind).pipe_into(list)
        return pl.Series(cls.__name__, values, dtype=pl.Enum(values))

    @classmethod
    def to_iter(cls, kind: Kind = "value") -> pc.Iter[str]:
        """Return the Enum members as a plain Python list.

        Example:
            >>> class MyEnum(Enum):
            ...     value1 = "value1"
            ...     value2 = "value2"
            ...     value3 = "value3"
            >>> MyEnum.to_list()
            ['value1', 'value2', 'value3']
        """
        match kind:
            case "value":
                return pc.Iter(member.value for member in cls)
            case "name":
                return pc.Iter(member.name for member in cls)

    @classmethod
    def to_list(cls, kind: Kind = "value") -> pc.Seq[str]:
        """Return the Enum members as a pychain Seq.

        Syntactic sugar for `to_iter().to_seq()`.
        """
        return cls.to_iter(kind).to_list()

    @classmethod
    def to_dtype(cls, kind: Kind = "value") -> pl.Enum:
        """Return a Polars Enum dtype for this Enum.

        Example:
            >>> class MyEnum(Enum):
            ...     a = "a"
            ...     b = "b"
            >>> MyEnum.to_dtype()
            Enum(categories=['a', 'b'])
        """
        return pl.Enum(cls.to_iter(kind).unwrap())
