from collections.abc import Sequence
from enum import StrEnum

import polars as pl


class Enum(StrEnum):
    @classmethod
    def to_series(cls) -> pl.Series:
        """Convert the Enum members to a Polars Series.

        Example:
            >>> class MyEnum(Enum):
            ...     value1 = "value1"
            ...     value2 = "value2"
            ...     value3 = "value3"
            >>> MyEnum.to_series().to_list()
            ['value1', 'value2', 'value3']
        """
        return pl.Series(
            cls.__name__, [member.value for member in cls], dtype=cls.to_dtype()
        )

    @classmethod
    def to_list(cls) -> list[str]:
        """Return the Enum members as a plain Python list.

        Example:
            >>> class MyEnum(Enum):
            ...     value1 = "value1"
            ...     value2 = "value2"
            ...     value3 = "value3"
            >>> MyEnum.to_list()
            ['value1', 'value2', 'value3']
        """
        return [member.value for member in cls]

    @classmethod
    def to_dtype(cls) -> pl.Enum:
        """Return a Polars Enum dtype for this Enum.

        Example:
            >>> class MyEnum(Enum):
            ...     a = "a"
            ...     b = "b"
            >>> MyEnum.to_dtype()
            Enum(categories=['a', 'b'])
        """
        return pl.Enum(cls)

    @classmethod
    def from_df(cls, data: pl.DataFrame | pl.LazyFrame, name: str) -> "Enum":
        """Create a dynamic Enum from values present in a DataFrame column.

        Example:
            >>> import polars as pl
            >>> df = pl.DataFrame({"col": ["b", "a", "b", "c"]})
            >>> Enum.from_df(df, "col").to_list()
            ['a', 'b', 'c']
        """
        return Enum(
            name,
            data.lazy()
            .select(pl.col(name))
            .unique()
            .sort(name)
            .cast(pl.String)
            .collect()
            .get_column(name)
            .to_list(),
        )

    @classmethod
    def from_sequence(cls, data: pl.Series | Sequence[str], name: str) -> "Enum":
        """Create a dynamic Enum from a sequence or Series.

        Example:
            >>> Enum.from_sequence(["value3", "value1", "value2", "value1"], name="X").to_list()
            ['value1', 'value2', 'value3']
        """
        if not isinstance(data, pl.Series):
            data = pl.Series(name, data)

        return Enum(name, data.unique().sort().cast(pl.String).to_list())
