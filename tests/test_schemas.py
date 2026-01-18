"""Tests for Schema inheritance and interactions with Table/DataBase inside a Folder."""

from pathlib import Path

import pyochain as pc
import pytest

import framelib as fl


def test_schema_inheritance_and_table_interaction(tmp_path: Path) -> None:
    """Verify schema inheritance and DataBase/Table interactions inside a Folder."""

    class BaseS(fl.Schema):
        a = fl.Int64()

    class DerivedS(BaseS):
        b = fl.String()

    # both columns must be present in the derived schema
    keys = set(DerivedS.schema().keys())
    assert "a" in keys
    assert "b" in keys

    class MyDB(fl.DataBase):
        t = fl.Table(model=DerivedS)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    db = Project.db

    with db:
        assert db.is_connected
        # table should have received the same connexion instance
        assert db.t.connexion.unwrap() is db.connexion

    # to_sql should contain both column names
    sql = DerivedS.to_sql()
    assert '"a"' in sql
    assert '"b"' in sql


def test_schema_cast_and_multi_column_pk(tmp_path: Path) -> None:
    """Verify behavior when a Schema declares multiple primary_key columns."""
    with pytest.raises(pc.ResultUnwrapError):  # noqa: PT012

        class S(fl.Schema):
            a = fl.Int64(primary_key=True)
            b = fl.Int64(primary_key=True)
            val = fl.String()

        class DB(fl.DataBase):
            t = fl.Table(model=S)

        class Project(fl.Folder):  # pyright: ignore[reportUnusedClass] # type: ignore[[unused-ignore]]
            __source__ = Path(tmp_path)
            db = DB()
