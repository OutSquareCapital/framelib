from collections.abc import Sequence
from functools import partial

import dataframely as dy
import polars as pl

from ._files import File


class Parquet[T: dy.Schema](File[T]):
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
    _with_suffix: bool = False
    __slots__ = ("_partition_by",)

    def __init__(
        self, partition_by: str | Sequence[str], model: type[T] = dy.Schema
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


class CSV[T: dy.Schema](File[T]):
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


class NDJson[T: dy.Schema](File[T]):
    @property
    def scan(self):
        return partial(pl.scan_ndjson, self.source)

    @property
    def read(self):
        return partial(pl.read_ndjson, self.source)

    @property
    def write(self):
        return partial(pl.DataFrame.write_ndjson, file=self.source)


class Json[T: dy.Schema](File[T]):
    @property
    def read(self):
        return partial(pl.read_json, self.source)

    @property
    def write(self):
        return partial(pl.DataFrame.write_json, file=self.source)
