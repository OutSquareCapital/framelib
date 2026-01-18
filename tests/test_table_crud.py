"""CRUD and conflict tests for Table within a Folder/DataBase."""

from pathlib import Path

import polars as pl
import pyochain as pc
import pytest

import framelib as fl


def test_table_crud_and_conflicts(tmp_path: Path) -> None:
    """Test CRUD operations and primary-key conflict behaviors on a Table."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        name = fl.String()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)
    db = Project.db

    df1 = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    with db:
        # create or replace
        db.t.create_or_replace_from(df1)
        res = db.t.read()
        assert res["id"].to_list() == [1, 2]
        assert res["name"].to_list() == ["a", "b"]

        # insert_or_replace: update id=2 and add id=3
        df2 = pl.DataFrame({"id": [2, 3], "name": ["bb", "c"]})
        db.t.insert_or_replace(df2)
        res2 = db.t.read().sort("id")
        expected_ids = [1, 2, 3]
        updated_id = 2
        assert res2["id"].to_list() == expected_ids
        assert res2.filter(pl.col("id") == updated_id)["name"].to_list() == ["bb"]

        # insert_or_ignore: try to insert duplicate id=3 (should be ignored)
        df3 = pl.DataFrame({"id": [3], "name": ["ccc"]})
        db.t.insert_or_ignore(df3)
        res3 = db.t.read().sort("id")
        ignored_id = 3
        assert res3.filter(pl.col("id") == ignored_id)["name"].to_list() == ["c"]

        # truncate
        db.t.truncate()
        res4 = db.t.read()
        assert res4.height == 0

        # recreate then drop
        db.t.create_or_replace_from(df1)
        db.t.drop()
        import duckdb as _duckdb

        with pytest.raises(_duckdb.CatalogException):
            _ = db.t.relation


def test_table_access_outside_connection_raises(tmp_path: Path) -> None:
    """Accessing table relation outside an active connection raises unwrap error."""

    class S(fl.Schema):
        id = fl.Int64()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with pytest.raises(pc.ResultUnwrapError):
        _ = Project.db.t.relation
