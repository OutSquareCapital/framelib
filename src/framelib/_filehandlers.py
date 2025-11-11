from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path
from typing import Any, Final

import duckdb
import polars as pl

from ._core import Entry
from ._schema import Schema


class File[T: Schema](Entry[T, Path], ABC):
    _is_file: Final[bool] = True
    _with_suffix: bool = True

    def __init__(self, model: type[T] = Schema) -> None:
        self.model = model

    def __set_source__(self, source: Path | str) -> None:
        self.source = Path(source, self._name)
        if self.__class__._with_suffix:
            self.source = self.source.with_suffix(f".{self._cls_name.lower()}")

    @property
    @abstractmethod
    def read(self) -> Callable[..., pl.DataFrame]:
        raise NotImplementedError

    @property
    def scan(self) -> Any:
        raise NotImplementedError

    @property
    @abstractmethod
    def write(self) -> Any:
        raise NotImplementedError

    def read_cast(self) -> pl.DataFrame:
        """
        Read the file and cast it to the defined schema.
        """
        return self.read().pipe(self.model.cast).to_native().collect()

    def scan_cast(self) -> pl.LazyFrame:
        """
        Scan the file and cast it to the defined schema.
        """
        return self.scan().pipe(self.model.cast).to_native()

    def write_cast(
        self, df: pl.LazyFrame | pl.DataFrame, *args: Any, **kwargs: Any
    ) -> None:
        """
        Cast the dataframe to the defined schema and write it to the file.
        """
        self.model.cast(df.lazy().collect()).to_native().pipe(
            self.write, *args, **kwargs
        )


class Parquet[T: Schema](File[T]):
    @property
    def scan(self):
        return partial(pl.scan_parquet, self.source)

    @property
    def read(self):
        return partial(pl.read_parquet, self.source)

    def read_schema(self) -> dict[str, pl.DataType]:
        """
        Get the schema of a Parquet file without reading data.

        If you would like to read the schema of a cloud file with authentication
        configuration, it is recommended use `scan_parquet` - e.g.
        `scan_parquet(..., storage_options=...).collect_schema()`.

        Parameters
        ----------
        source
            Path to a file or a file-like object (by "file-like object" we refer to objects
            that have a `read()` method, such as a file handler like the builtin `open`
            function, or a `BytesIO` instance). For file-like objects, the stream position
            may not be updated accordingly after reading.

        Returns
        -------
        dict
            Dictionary mapping column names to datatypes

        See Also
        --------
        scan_parquet
        """
        return pl.read_parquet_schema(self.source)

    def read_metadata(self) -> dict[str, str]:
        """
        Get file-level custom metadata of a Parquet file without reading data.

        .. warning::
            This functionality is considered **experimental**. It may be removed or
            changed at any point without it being considered a breaking change.

        Parameters
        ----------
        source
            Path to a file or a file-like object (by "file-like object" we refer to objects
            that have a `read()` method, such as a file handler like the builtin `open`
            function, or a `BytesIO` instance). For file-like objects, the stream position
            may not be updated accordingly after reading.

        Returns
        -------
        dict
            Dictionary with the metadata. Empty if no custom metadata is available.
        """
        return pl.read_parquet_metadata(self.source)

    @property
    def write(self):
        return partial(pl.DataFrame.write_parquet, file=self.source)


class ParquetPartitioned[T: Schema](Parquet[T]):
    """A Parquet file that is partitioned by one or more columns."""

    _with_suffix: bool = False
    __slots__ = ("_partition_by",)

    def __init__(
        self, partition_by: str | Sequence[str], model: type[T] = Schema
    ) -> None:
        self.model: type[T] = model
        self._partition_by: str | Sequence[str] = partition_by

    @property
    def write(self):
        return partial(
            pl.DataFrame.write_parquet,
            file=self.source,
            partition_by=self._partition_by,
        )


class CSV[T: Schema](File[T]):
    @property
    def scan(self):
        return partial(pl.scan_csv, self.source)

    @property
    def read(self):
        return partial(pl.read_csv, self.source)

    @property
    def read_batched(self):
        return partial(pl.read_csv_batched, self.source)

    @property
    def write(self):
        return partial(pl.DataFrame.write_csv, file=self.source)


class NDJson[T: Schema](File[T]):
    @property
    def scan(self):
        return partial(pl.scan_ndjson, self.source)

    @property
    def read(self):
        return partial(pl.read_ndjson, self.source)

    @property
    def write(self):
        return partial(pl.DataFrame.write_ndjson, file=self.source)


class Json[T: Schema](File[T]):
    """
    Json file handler
    """

    @property
    def scan(self) -> pl.LazyFrame:
        return duckdb.read_json(self.source.as_posix()).pl(lazy=True)  # type: ignore

    @property
    def read(self):
        return partial(pl.read_json, self.source)

    @property
    def write(self):
        return partial(pl.DataFrame.write_json, file=self.source)
