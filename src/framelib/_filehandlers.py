from __future__ import annotations

from abc import ABC, abstractmethod
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, override

import polars as pl

from ._core import Entry

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from ._schema import Schema


class File(Entry, ABC):
    """A `File` represents a file in a folder.

    It's an `Entry` in a `Folder`.

    It can be associated with a `Schema` of `Columns` to define the structure of the data within the file.

    Properties:
        - `read`: A callable that reads the file and returns a `pl.DataFrame`.
        - `scan`: A callable that scans the file and returns a `pl.LazyFrame`.
        - `write`: A callable that writes a `pl.DataFrame` to the file.

    Note:
        The read/write/scan are implemented as properties who return partials as a way to keep original documentation, autocompletion and full compatibility with polars functions.

        But they concretely act just like methods (i.e., you call them with parentheses and arguments).

        Hence, framelib sole responsibility is to provide the correct file path as the first argument.

    Args:
        schema (type[T]): The schema schema associated with the file.
    """

    __slots__ = ()  # pyright: ignore[reportUnannotatedClassAttribute, reportIncompatibleUnannotatedOverride]

    @override
    def __set_source__(self, source: Path | str) -> None:
        self.__source__: Path = Path(source, self._name).with_suffix(
            f".{self.__class__.__name__.lower()}"
        )

    @property
    @abstractmethod
    def read(self) -> Callable[..., pl.DataFrame]:
        raise NotImplementedError

    @property
    @abstractmethod
    def scan(self) -> Callable[..., pl.LazyFrame]:
        raise NotImplementedError

    @property
    @abstractmethod
    def write(self) -> Callable[..., None]:
        raise NotImplementedError


class Parquet(File):
    """A Parquet file handler."""

    __slots__ = ()  # pyright: ignore[reportUnannotatedClassAttribute]

    @property
    @override
    def scan(self):  # noqa: ANN202
        return partial(pl.scan_parquet, self.source, schema=self.schema.to_pl())

    @property
    @override
    def read(self):  # noqa: ANN202
        return partial(pl.read_parquet, self.source, schema=self.schema.to_pl())

    @property
    @override
    def write(self):  # noqa: ANN202
        return partial(pl.DataFrame.write_parquet, file=self.source)


class ParquetPartitioned(Parquet):
    """A Parquet file that is partitioned by one or more columns.

    A partitioned Parquet file is organized into separate directories for each unique value of the partitioning column(s).

    Hence, this file is in fact a folder containing multiple Parquet files.

    However, polars already handles this abstraction for us, so we can treat it as a single file.

    Args:
        partition_by (str | Sequence[str]): The column(s) to partition by.
        schema (type[Schema], optional): The schema schema associated with the file. Defaults to Schema.
    """

    _partition_by: str | Sequence[str]
    __slots__ = ("_partition_by",)  # pyright: ignore[reportUnannotatedClassAttribute, reportIncompatibleUnannotatedOverride]

    def __init__(self, partition_by: str | Sequence[str], schema: type[Schema]) -> None:
        self._partition_by = partition_by
        super().__init__(schema)

    @override
    def __set_source__(self, source: Path | str) -> None:
        self.__source__: Path = Path(source, self._name)

    @property
    @override
    def write(self):  # noqa: ANN202
        return partial(
            pl.DataFrame.write_parquet,
            file=self.source,
            partition_by=self._partition_by,
        )


class CSV(File):
    """Represents a CSV file.

    Acts as an interface with methods to scan, read, read in batches, and write CSV data using Polars functions.
    """

    __slots__ = ()  # pyright: ignore[reportUnannotatedClassAttribute]

    @property
    @override
    def scan(self):  # noqa: ANN202
        return partial(pl.scan_csv, self.source, schema=self.schema.to_pl())

    @property
    @override
    def read(self):  # noqa: ANN202
        return partial(pl.read_csv, self.source, schema=self.schema.to_pl())

    @property
    @override
    def write(self):  # noqa: ANN202
        return partial(pl.DataFrame.write_csv, file=self.source)


class NDJson(File):
    """Represents a file handler for newline-delimited JSON (NDJSON) files.

    Provides properties to scan, read, and write NDJSON data using Polars functions.
    """

    __slots__ = ()  # pyright: ignore[reportUnannotatedClassAttribute]

    @property
    @override
    def scan(self):  # noqa: ANN202
        return partial(pl.scan_ndjson, self.source, schema=self.schema.to_pl())

    @property
    @override
    def read(self):  # noqa: ANN202
        return partial(pl.read_ndjson, self.source, schema=self.schema.to_pl())

    @property
    @override
    def write(self):  # noqa: ANN202
        return partial(pl.DataFrame.write_ndjson, file=self.source)


class Json(File):
    r"""Represents a JSON file.

    Acts as an interface with methods to scan the file as a pl.LazyFrame using DuckDB, and to read from or write to the file using Polars.

    Note:
        The `scan` property is using DuckDB as a backend.
        This is due to the fact that Polars does not support lazy reading of JSON files directly.
        It is to be determined whether this approach is truly efficient for large JSON files
        compared to reading the entire file into memory.
    """

    __slots__ = ()  # pyright: ignore[reportUnannotatedClassAttribute]

    @property
    @override
    def scan(self) -> Callable[[], pl.LazyFrame]:
        raise NotImplementedError

    @property
    @override
    def read(self):  # noqa: ANN202
        return partial(pl.read_json, self.source, schema=self.schema.to_pl())

    @property
    @override
    def write(self):  # noqa: ANN202
        return partial(pl.DataFrame.write_json, file=self.source)
