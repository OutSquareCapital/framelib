from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path

import duckdb
import polars as pl

from ._core import Entry
from ._database import Schema


class File[T: Schema](Entry[T], ABC):
    """A `File` represents a file in a folder.

    It's an `Entry` in a `Folder`.

    It can be associated with a `Schema` of `Columns` to define the structure of the data within the file.

    Properties:
        - `read`: A callable that reads the file and returns a `pl.DataFrame`.
        - `scan`: A callable that scans the file and returns a `pl.LazyFrame`.
        - `write`: A callable that writes a `pl.DataFrame` to the file.

    Note:
        The read/write/scan are implemented as properties who return partials as a way to keep original documentation and full compatibility with polars functions.

        But they concretely act just like methods (i.e., you call them with parentheses and arguments).

        Hence, framelib sole responsibility is to provide the correct file path as the first argument.

    Args:
        model (type[T]): The schema model associated with the file.
    """

    _with_suffix: bool = True

    def __init__(self, model: type[T] = Schema) -> None:
        self._model = model

    def __set_source__(self, source: Path | str) -> None:
        self.__source__ = Path(source, self._name)
        if self.__class__._with_suffix:
            self.__source__ = self.__source__.with_suffix(
                f".{self.__class__.__name__.lower()}",
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

    def read_cast(self) -> pl.DataFrame:
        """Read the file and cast it to the defined schema.

        Returns:
            pl.DataFrame: The read and casted DataFrame.
        """
        return self.read().pipe(self.model.cast).to_native().collect()

    def scan_cast(self) -> pl.LazyFrame:
        """Scan the file and cast it to the defined schema.

        Returns:
            pl.LazyFrame: The scanned and casted LazyFrame.
        """
        return self.scan().pipe(self.model.cast).to_native()


class Parquet[T: Schema](File[T]):
    """A Parquet file handler."""

    @property
    def scan(self):  # noqa: ANN202
        return partial(pl.scan_parquet, self.source)

    @property
    def read(self):  # noqa: ANN202
        return partial(pl.read_parquet, self.source)

    @property
    def write(self):  # noqa: ANN202
        return partial(pl.DataFrame.write_parquet, file=self.source)


class ParquetPartitioned[T: Schema](Parquet[T]):
    """A Parquet file that is partitioned by one or more columns.

    A partitioned Parquet file is organized into separate directories for each unique value of the partitioning column(s).

    Hence, this file is in fact a folder containing multiple Parquet files.

    However, polars already handles this abstraction for us, so we can treat it as a single file.

    Args:
        partition_by (str | Sequence[str]): The column(s) to partition by.
        model (type[T], optional): The schema model associated with the file. Defaults to Schema.
    """

    _with_suffix: bool = False
    __slots__ = ("_partition_by",)

    def __init__(
        self,
        partition_by: str | Sequence[str],
        model: type[T] = Schema,
    ) -> None:
        self.model: type[T] = model
        self._partition_by: str | Sequence[str] = partition_by

    @property
    def write(self):  # noqa: ANN202
        return partial(
            pl.DataFrame.write_parquet,
            file=self.source,
            partition_by=self._partition_by,
        )


class CSV[T: Schema](File[T]):
    """Represents a CSV file.

    Acts as an interface with methods to scan, read, read in batches, and write CSV data using Polars functions.
    """

    @property
    def scan(self):  # noqa: ANN202
        return partial(pl.scan_csv, self.source)

    @property
    def read(self):  # noqa: ANN202
        return partial(pl.read_csv, self.source)

    @property
    def read_batched(self):  # noqa: ANN202
        return partial(pl.read_csv_batched, self.source)

    @property
    def write(self):  # noqa: ANN202
        return partial(pl.DataFrame.write_csv, file=self.source)


class NDJson[T: Schema](File[T]):
    """Represents a file handler for newline-delimited JSON (NDJSON) files.

    Provides properties to scan, read, and write NDJSON data using Polars functions.
    """

    @property
    def scan(self):  # noqa: ANN202
        return partial(pl.scan_ndjson, self.source)

    @property
    def read(self):  # noqa: ANN202
        return partial(pl.read_ndjson, self.source)

    @property
    def write(self):  # noqa: ANN202
        return partial(pl.DataFrame.write_ndjson, file=self.source)


class Json[T: Schema](File[T]):
    r"""Represents a JSON file.

    Acts as an interface with methods to scan the file as a pl.LazyFrame using DuckDB, and to read from or write to the file using Polars.

    Note:
        The `scan` property is using DuckDB as a backend.
        This is due to the fact that Polars does not support lazy reading of JSON files directly.
        It is to be determined whether this approach is truly efficient for large JSON files
        compared to reading the entire file into memory.
    """

    @property
    def scan(self) -> Callable[[], pl.LazyFrame]:
        def _read_json_to_lf(  # noqa: PLR0913
            columns: dict[str, str] | None = None,
            sample_size: int | None = None,
            maximum_depth: int | None = None,
            records: str | None = None,
            date_format: str | None = None,
            timestamp_format: str | None = None,
            compression: str | None = None,
            maximum_object_size: int | None = None,
            field_appearance_threshold: float | None = None,
            map_inference_threshold: int | None = None,
            maximum_sample_files: int | None = None,
            fmt: str | None = None,
            *,
            ignore_errors: bool | None = None,
            convert_strings_to_integers: bool | None = None,
            filename: bool | str | None = None,
            hive_partitioning: bool | None = None,
            union_by_name: bool | None = None,
            hive_types: dict[str, str] | None = None,
            hive_types_autocast: bool | None = None,
        ) -> pl.LazyFrame:
            r"""Lazily load a JSON (or NDJSON) file via DuckDB and return a Polars LazyFrame.

            This method uses DuckDB `read_json` function under the hood,
            to support lazy scanning of JSON files
            (Polars itself does not currently support lazy JSON reads).

            The parameters mirror those documented for DuckDB JSON loader, see:
            https://duckdb.org/docs/stable/data/json/loading_json

            Args:
                columns (dict[str, str] | None): Optional explicit mapping from JSON field names to DuckDB types.
                    If omitted, schema is inferred automatically. (See `columns`).
                sample_size (int | None): Number of rows to sample for schema inference. Defaults apply if None.
                maximum_depth (int | None): Maximum nesting depth to consider during schema inference.
                records (str | None): Whether the JSON uses a records format or array/unstructured (e.g., 'auto', 'true', 'false').
                    Whether the JSON uses a records format or array/unstructured (e.g., 'auto', 'true', 'false').
                date_format (str | None): Custom date parsing format.
                timestamp_format (str | None): Custom timestamp parsing format.
                compression (str | None): Compression type (e.g., 'gzip', 'zstd', 'auto_detect').
                maximum_object_size (int | None): Maximum size in bytes of a JSON object for inference.
                field_appearance_threshold (float | None): Threshold fraction of sample rows in which a field must appear to be included.
                map_inference_threshold (int | None): Maximum distinct keys before promoting a field to MAP type.
                maximum_sample_files (int | None): Maximum number of files to sample when reading from a file set/glob.
                fmt (str | None): Format alias; used when non-standard JSON format is required.
                ignore_errors (bool | None): Whether to ignore parse errors (only valid when `records='newline_delimited'`).
                convert_strings_to_integers (bool | None): Whether strings that look like integers should be cast to integer types.
                filename (bool | str | None): If `True`, adds a `filename` virtual column; if string, uses that as column name.
                hive_partitioning (bool | None): Enable Hive-style partitioning inference on paths.
                union_by_name (bool | None): If True, unify schemas of multiple files by matching field names.
                hive_types (dict[str, str] | None): Explicit types for Hive partition columns.
                hive_types_autocast (bool | None): Whether to auto-cast Hive partition values to the specified types.

            Returns:
                pl.LazyFrame:
                    A Polars LazyFrame representing the JSON file(s) read via DuckDB,
                    supporting lazy evaluation of downstream Polars operations.
            """
            return duckdb.read_json(
                self.source.as_posix(),
                columns=columns,
                sample_size=sample_size,
                maximum_depth=maximum_depth,
                records=records,
                format=fmt,
                date_format=date_format,
                timestamp_format=timestamp_format,
                compression=compression,
                maximum_object_size=maximum_object_size,
                ignore_errors=ignore_errors,
                convert_strings_to_integers=convert_strings_to_integers,
                field_appearance_threshold=field_appearance_threshold,
                map_inference_threshold=map_inference_threshold,
                maximum_sample_files=maximum_sample_files,
                filename=filename,
                hive_partitioning=hive_partitioning,
                union_by_name=union_by_name,
                hive_types=hive_types,
                hive_types_autocast=hive_types_autocast,
            ).pl(lazy=True)

        return _read_json_to_lf

    @property
    def read(self):  # noqa: ANN202
        return partial(pl.read_json, self.source)

    @property
    def write(self):  # noqa: ANN202
        return partial(pl.DataFrame.write_json, file=self.source)
