import polars as pl

from ._schemas import IODescriptor, Schema


class CSV(Schema):
    """
    Schema for CSV files, providing methods to read and scan using polars.

    Example:
        >>> from pathlib import Path
        >>> class MyFile(CSV):
        ...     __directory__ = Path("tests").joinpath("data_csv")
        >>> MyFile.path(True).touch()
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
        >>> from pathlib import Path
        >>> class MyFile(Parquet):
        ...     __directory__ = Path("tests").joinpath("data_parquet")
        >>> MyFile.path(True).touch()
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
        >>> from pathlib import Path
        >>> class MyFile(NDJSON):
        ...     __directory__ = Path("tests").joinpath("data_ndjson")
        >>> MyFile.path(True).touch()
        >>> MyFile.show_tree()
        tests\\data_ndjson
        └── MyFile.ndjson
    """

    __ext__ = ".ndjson"
    read = IODescriptor(pl.read_ndjson)
    scan = IODescriptor(pl.scan_ndjson)


class JSON(Schema):
    """
    Schema for JSON files, providing methods to read using polars.

    Example:
        >>> from pathlib import Path
        >>> class MyFile(JSON):
        ...     __directory__ = Path("tests").joinpath("data_json")
        >>> MyFile.path(True).touch()
        >>> MyFile.show_tree()
        tests\\data_json
        └── MyFile.json
    """

    __ext__ = ".json"
    read = IODescriptor(pl.read_json)
