from . import _duckdb as duck
from ._core import Folder
from ._lib import CSV, Json, NDJson, Parquet, ParquetPartitioned

__all__ = [
    "CSV",
    "Json",
    "NDJson",
    "Parquet",
    "ParquetPartitioned",
    "Folder",
    "duck",
]
