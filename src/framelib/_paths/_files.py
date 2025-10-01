from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path
from typing import Any, Final, Self, TypeGuard

import dataframely as dy
import duckdb
import narwhals as nw
import polars as pl
import pychain as pc
from narwhals.typing import IntoFrame, IntoLazyFrame

from .._core import SourceUser
from .._schema import Schema


class FileReader[T: dy.Schema](SourceUser[T]):
    """
    File reader class for different file formats.

    The path attribute will be set automatically when used in a Folder subclass, using the variable name as the filename, and the subclass name as the file extension.

    if glob is set to True, the extension will be ignored, and the path will be generated without extension.

    This is primarily useful for partitioned parquet files.

    The read and scan properties returns partial functions from polars, with the path already set.
    """

    source: Path
    extension: str
    schema: type[T]
    _is_file_reader: Final[bool] = True
    __slots__ = ("source", "extension", "schema")

    def __init__(self, schema: type[T] = dy.Schema) -> None:
        self.extension = f".{self.__class__.__name__.lower()}"
        self.schema: type[T] = schema

    def __from_source__(self, source: Path | str, name: str) -> None:
        self.source = Path(source, name).with_suffix(self.extension)

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[FileReader[Any]]:
        return getattr(obj, "_is_file_reader", False) is True

    def iter_dir(self) -> pc.Iter[Path]:
        """
        Returns a pychain iterator over the files in the directory containing this schema's file.
        """
        return pc.Iter(self.source.iterdir())

    @property
    @abstractmethod
    def read(self) -> Callable[..., pl.DataFrame]:
        raise NotImplementedError

    @property
    def scan(self) -> Callable[..., pl.LazyFrame]:
        raise NotImplementedError

    @property
    @abstractmethod
    def write(self) -> Any:
        raise NotImplementedError

    def read_cast(self) -> dy.DataFrame[T]:
        """
        Read the file and cast it to the defined schema.
        """
        return self.read().pipe(self.schema.cast)

    def scan_cast(self) -> dy.LazyFrame[T]:
        """
        Scan the file and cast it to the defined schema.
        """
        return self.scan().pipe(self.schema.cast)

    def write_cast(
        self, df: pl.LazyFrame | pl.DataFrame, *args: Any, **kwargs: Any
    ) -> None:
        """
        Cast the dataframe to the defined schema and write it to the file.
        """
        self.schema.cast(df.lazy().collect()).pipe(self.write, *args, **kwargs)


class Table[T: Schema](SourceUser[T]):
    _name: str
    _con: duckdb.DuckDBPyConnection
    schema: type[T]
    _is_table: Final[bool] = True
    __slots__ = ("_name", "_con", "schema")

    def __init__(self, schema: type[T] = Schema) -> None:
        self.schema = schema

    @staticmethod
    def __identity__(obj: Any) -> TypeGuard[Table[Any]]:
        return getattr(obj, "_is_table", False)

    def __from_source__(self, source: duckdb.DuckDBPyConnection, name: str) -> None:
        self.source = source
        self._name = name

    def with_connexion(self, con: duckdb.DuckDBPyConnection) -> Self:
        self._con = con
        return self

    def read(self) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
        return nw.from_native(self._con.table(self._name))

    @property
    def name(self) -> str:
        return self._name

    def write(self, df: IntoFrame | IntoLazyFrame) -> None:
        df_to_write = nw.from_native(df).lazy().collect().to_native()  # type: ignore # noqa
        self._con.execute(
            f"CREATE OR REPLACE TABLE {self._name} AS SELECT * FROM df_to_write"
        )


class Parquet[T: dy.Schema](FileReader[T]):
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


class ParquetPartitioned[T: dy.Schema](Parquet[T]):
    __slots__ = ("_partition_by",)

    def __init__(
        self, partition_by: str | Sequence[str], schema: type[T] = dy.Schema
    ) -> None:
        self.schema: type[T] = schema
        self.extension = ""
        self._partition_by: str | Sequence[str] = partition_by

    @property
    def write(self):
        return partial(
            pl.DataFrame.write_parquet,
            file=self.source,
            partition_by=self._partition_by,
        )


class CSV[T: dy.Schema](FileReader[T]):
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


class NDJson[T: dy.Schema](FileReader[T]):
    @property
    def scan(self):
        return partial(pl.scan_ndjson, self.source)

    @property
    def read(self):
        return partial(pl.read_ndjson, self.source)

    @property
    def write(self):
        return partial(pl.DataFrame.write_ndjson, file=self.source)


class Json[T: dy.Schema](FileReader[T]):
    @property
    def read(self):
        return partial(pl.read_json, self.source)

    @property
    def write(self):
        return partial(pl.DataFrame.write_json, file=self.source)
