"""Tests for DataBase connection decorator usage within a Folder."""

from pathlib import Path

import framelib as fl


def test_db_decorator_connects_and_closes(tmp_path: Path) -> None:
    """Using a DataBase inside a Folder ensures sources are set automatically."""

    class UserSchema(fl.Schema):
        id = fl.Int64(primary_key=True)

    class MyDB(fl.DataBase):
        users = fl.Table(model=UserSchema)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    # ensure folder exists so DuckDB can create the file
    Project.source().mkdir(parents=True, exist_ok=True)

    db = Project.db

    @db
    def fn_simple() -> None:
        assert db.is_connected
        assert db.connexion is not None
        assert db.users.connexion.unwrap() is db.connexion

    fn_simple()
    assert not db.is_connected


def test_nested_and_multiple_db_decorators(tmp_path: Path) -> None:
    """Multiple DBs as Folder entries: nested and stacked decorators."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)

    class DBA(fl.DataBase):
        t = fl.Table(model=S)

    class DBB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        dba = DBA()
        dbb = DBB()

    # ensure folder exists so DuckDB can create the files
    Project.source().mkdir(parents=True, exist_ok=True)

    dba = Project.dba
    dbb = Project.dbb

    @dba
    @dbb
    def both_decorated() -> None:
        assert dba.is_connected
        assert dbb.is_connected
        assert dba.t.connexion.unwrap() is dba.connexion
        assert dbb.t.connexion.unwrap() is dbb.connexion

    both_decorated()
    assert not dba.is_connected
    assert not dbb.is_connected

    @dba
    def outer() -> None:
        assert dba.is_connected
        inner()

    @dbb
    def inner() -> None:
        assert dbb.is_connected
        # when inner is called from outer, outer's db should still be connected
        assert dba.is_connected

    outer()
    assert not dba.is_connected
    assert not dbb.is_connected
