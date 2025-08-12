import polars as pl

from ._schemas import IODescriptor, Schema


class CSV(Schema):
    __ext__ = ".csv"
    read = IODescriptor(pl.read_csv)
    scan = IODescriptor(pl.scan_csv)


class Parquet(Schema):
    __ext__ = ".parquet"
    read = IODescriptor(pl.read_parquet)
    scan = IODescriptor(pl.scan_parquet)


class NDJSON(Schema):
    __ext__ = ".ndjson"
    read = IODescriptor(pl.read_ndjson)
    scan = IODescriptor(pl.scan_ndjson)
