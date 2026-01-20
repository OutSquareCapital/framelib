"""Tests for File handlers: Parquet and CSV read/write through `Folder` entries."""

from pathlib import Path

import polars as pl

import framelib as fl


def test_parquet_write_and_read(tmp_path: Path) -> None:
    """Write and read a Parquet file via the `Parquet` File handler."""

    class S(fl.Schema):
        id = fl.Int64()
        name = fl.String()

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        data = fl.Parquet(schema=S)

    Project.source().mkdir(parents=True, exist_ok=True)

    # write (callable expects the DataFrame as first arg)
    Project.data.write(pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"]}))

    # read (no args)
    df2 = Project.data.read()
    assert df2.shape == (2, 2)
    assert df2.get_column("name").to_list() == ["alice", "bob"]


def test_csv_write_and_read(tmp_path: Path) -> None:
    """Write and read a CSV file via the `CSV` File handler."""

    class S(fl.Schema):
        id = fl.Int64()
        val = fl.String()

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        data = fl.CSV(schema=S)

    Project.source().mkdir(parents=True, exist_ok=True)

    pl.DataFrame({"id": [10, 20], "val": ["x", "y"]}).pipe(Project.data.write)
    df2 = Project.data.read()
    assert df2.shape == (2, 2)
    assert df2.get_column("id").to_list() == [10, 20]
