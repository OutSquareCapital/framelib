from enum import StrEnum
from typing import Literal, Self

import polars as pl

Kind = Literal["name", "value"]


class Enum(StrEnum):
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
        values: list[str] = cls.to_list(kind)
        return pl.Series(cls.__name__, values, dtype=pl.Enum(values))

    @classmethod
    def to_list(cls, kind: Kind = "value") -> list[str]:
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
                return [member.value for member in cls]
            case "name":
                return [member.name for member in cls]

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
        return pl.Enum(cls.to_list(kind))

    @classmethod
    def from_df(cls, data: pl.DataFrame | pl.LazyFrame, name: str) -> Self:
        """Create a dynamic Enum from values present in a DataFrame column.

        Example:
            >>> import polars as pl
            >>> df = pl.DataFrame({"col": ["b", "a", "b", "c"]})
            >>> Enum.from_df(df, "col").to_list()
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
    def from_series(cls, data: pl.Series) -> Self:
        """Create a dynamic Enum from a Series.

        Example:
            >>> Enum.from_series(pl.Series(["value3", "value1", "value2", "value1"])).to_list()
            ['value1', 'value2', 'value3']
        """
        return cls(data.name, data.unique().sort().to_list())
