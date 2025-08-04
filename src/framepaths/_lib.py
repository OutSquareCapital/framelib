from typing import Any

import dataframely as dy
import polars as pl
import pyarrow as pa
from dataframely.random import Generator

from ._schemas import IODescriptor, Schema


class CSVSchema(Schema):
    __ext__ = ".csv"
    read = IODescriptor(pl.read_csv)
    scan = IODescriptor(pl.scan_csv)


class ParquetSchema(Schema):
    __ext__ = ".parquet"
    read = IODescriptor(pl.read_parquet)
    scan = IODescriptor(pl.scan_parquet)


class NDJSONSchema(Schema):
    __ext__ = ".ndjson"
    read = IODescriptor(pl.read_ndjson)
    scan = IODescriptor(pl.scan_ndjson)


class Categorical(dy.Column):
    @property
    def dtype(self) -> pl.DataType:
        return pl.Categorical()

    def sqlalchemy_dtype(self, dialect: Any): ...
    @property
    def pyarrow_dtype(self) -> pa.DataType: ...
    def _sample_unchecked(self, generator: Generator, n: int) -> pl.Series: ...
