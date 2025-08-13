import polars as pl

from ._schemas import IODescriptor, Schema


class CSV(Schema):
    """
    Schema for CSV files, providing methods to read and scan using polars.

    Example:
        >>> class MyFile(CSV):
        ...     __directory__ = "tests/data_csv"
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
        >>> class MyFile(Parquet):
        ...     __directory__ = "tests/data_parquet"
        ...
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
        >>> class MyFile(NDJSON):
        ...     __directory__ = "tests/data_ndjson"
        ...
        >>> MyFile.path().touch()
        >>> MyFile.show_tree()
        tests\\data_ndjson
        └── MyFile.ndjson
    """

    __ext__ = ".ndjson"
    read = IODescriptor(pl.read_ndjson)
    scan = IODescriptor(pl.scan_ndjson)
