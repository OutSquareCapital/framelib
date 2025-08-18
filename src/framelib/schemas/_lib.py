from collections.abc import Sequence
from enum import StrEnum

import polars as pl

from ._schemas import IODescriptor, Schema


class Enum(StrEnum):
    @classmethod
    def to_list(cls) -> list[str]:
        return [member.value for member in cls]

    @classmethod
    def to_dtype(cls) -> pl.Enum:
        return pl.Enum(cls)

    @classmethod
    def from_df(cls, data: pl.DataFrame | pl.LazyFrame, col: str) -> "Enum":
        return Enum(
            col,
            data.lazy()
            .select(pl.col(col))
            .unique()
            .sort(col)
            .cast(pl.String)
            .collect()
            .get_column(col)
            .to_list(),
        )

    @classmethod
    def from_sequence(cls, data: pl.Series | Sequence[str]) -> "Enum":
        if not isinstance(data, pl.Series):
            data = pl.Series("FrameEnum", data)

        return Enum(data.name, data.unique().sort().cast(pl.String).to_list())


class CSV(Schema):
    """
    Schema for CSV files, providing methods to read and scan using polars.

    Example:
        >>> from framelib.schemas._schemas import directory
        >>> @directory("tests", "data_csv")
        ... class MyFile(CSV):
        ...     pass
        ...
        >>> MyFile.path().touch()
        >>> MyFile.show_tree()
        tests\\data_csv
        └── MyFile.csv
    """

    __ext__ = ".csv"
    read = IODescriptor(pl.read_csv)
    scan = IODescriptor(pl.scan_csv)


class Parquet(Schema):
    """
    Schema for Parquet files, providing methods to read and scan using polars.

    Example:
        >>> from framelib.schemas._schemas import directory
        >>> @directory("tests", "data_parquet")
        ... class MyFile(Parquet):
        ...     pass
        >>> MyFile.path().touch()
        >>> MyFile.show_tree()
        tests\\data_parquet
        └── MyFile.parquet
    """

    __ext__ = ".parquet"
    read = IODescriptor(pl.read_parquet)
    scan = IODescriptor(pl.scan_parquet)


class NDJSON(Schema):
    """
    Schema for NDJSON files, providing methods to read and scan using polars.

    Example:
        >>> from framelib.schemas._schemas import directory
        >>> @directory("tests", "data_ndjson")
        ... class MyFile(NDJSON):
        ...     pass
        ...
        >>> MyFile.path().touch()
        >>> MyFile.show_tree()
        tests\\data_ndjson
        └── MyFile.ndjson
    """

    __ext__ = ".ndjson"
    read = IODescriptor(pl.read_ndjson)
    scan = IODescriptor(pl.scan_ndjson)
