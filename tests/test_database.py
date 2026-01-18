"""Tests for DataBase connection decorator usage within a Folder."""

from pathlib import Path

import polars as pl
import pyochain as pc
import pytest

import framelib as fl

# ============================================================================
# Basic Connection Tests
# ============================================================================


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


def test_schema_inheritance_and_table_interaction(tmp_path: Path) -> None:
    """Verify schema inheritance and DataBase/Table interactions inside a Folder."""

    class BaseS(fl.Schema):
        a = fl.Int64()

    class DerivedS(BaseS):
        b = fl.String()

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


# ============================================================================
# Complex Connection Tests
# ============================================================================


def test_db_context_manager_reentrant(tmp_path: Path) -> None:
    """Database context manager supports reentrant usage."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)

    class MyDB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)
    db = Project.db

    with db:
        first_conn = db.connexion
        assert db.is_connected
        # Nested enter should keep the same connection
        with db:
            assert db.is_connected
            assert db.connexion is first_conn
        # After nested exit, should still be connected with same conn
        assert db.is_connected
        assert db.connexion is first_conn

    assert not db.is_connected


def test_db_decorator_chained_calls_same_db(tmp_path: Path) -> None:
    """Multiple decorated functions using same DB share connection state correctly."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        value = fl.String()

    class MyDB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)
    db = Project.db
    call_order: list[str] = []

    @db
    def first_op() -> None:
        call_order.append("first_start")
        db.t.create_or_replace_from(pl.DataFrame({"id": [1], "value": ["a"]}))
        second_op()
        call_order.append("first_end")

    @db
    def second_op() -> None:
        call_order.append("second_start")
        assert db.t.read().height == 1
        call_order.append("second_end")

    first_op()
    assert call_order == ["first_start", "second_start", "second_end", "first_end"]


def test_db_multiple_tables_single_transaction(tmp_path: Path) -> None:
    """Multiple tables can be modified in a single connection context."""

    class UserSchema(fl.Schema):
        id = fl.Int64(primary_key=True)
        name = fl.String()

    class OrderSchema(fl.Schema):
        id = fl.Int64(primary_key=True)
        user_id = fl.Int64()
        product = fl.String()

    class MyDB(fl.DataBase):
        users = fl.Table(model=UserSchema)
        orders = fl.Table(model=OrderSchema)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)
    db = Project.db

    with db:
        db.users.create_or_replace_from(
            pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        )
        db.orders.create_or_replace_from(
            pl.DataFrame(
                {
                    "id": [10, 20, 30],
                    "user_id": [1, 1, 2],
                    "product": ["Widget", "Gadget", "Thing"],
                }
            )
        )
        # Both tables accessible
        assert db.users.read().height == 2
        assert db.orders.read().height == 3


def test_db_connection_error_propagation(tmp_path: Path) -> None:
    """Errors in decorated functions close connection properly."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)

    class MyDB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)
    db = Project.db

    @db
    def failing_func() -> None:
        db.t.create_or_replace_from(pl.DataFrame({"id": [1]}))
        msg = "Intentional error"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="Intentional error"):
        failing_func()

    # Connection should be closed after exception
    assert not db.is_connected


def test_db_sql_execution_within_context(tmp_path: Path) -> None:
    """Raw SQL queries can be executed within connection context."""

    class S(fl.Schema):
        x = fl.Int64()
        y = fl.Float64()

    class MyDB(fl.DataBase):
        data = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)
    db = Project.db

    with db:
        db.data.create_or_replace_from(
            pl.DataFrame({"x": [1, 2, 3], "y": [1.5, 2.5, 3.5]})
        )
        result = db.sql("SELECT SUM(x) as total_x, AVG(y) as avg_y FROM data").collect()
        assert result.get_column("total_x").to_list()[0] == 6
        assert result.get_column("avg_y").to_list()[0] == pytest.approx(2.5)  # pyright: ignore[reportUnknownMemberType]


def test_db_show_tables_reflects_schema(tmp_path: Path) -> None:
    """show_tables reflects all tables created from schema."""

    class S1(fl.Schema):
        a = fl.Int64()

    class S2(fl.Schema):
        b = fl.String()

    class MyDB(fl.DataBase):
        table_one = fl.Table(model=S1)
        table_two = fl.Table(model=S2)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)
    db = Project.db

    with db:
        db.table_one.create_or_replace_from(pl.DataFrame({"a": [1]}))
        db.table_two.create_or_replace_from(pl.DataFrame({"b": ["x"]}))
        tables = db.show_tables().collect()
        table_names = pc.Set[str](tables.get_column("name"))
        assert "table_one" in table_names
        assert "table_two" in table_names


def test_db_concurrent_decorated_functions_different_dbs(tmp_path: Path) -> None:
    """Different databases in same folder can be used independently."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)

    class DBA(fl.DataBase):
        t = fl.Table(model=S)

    class DBB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        alpha = DBA()
        beta = DBB()

    Project.source().mkdir(parents=True, exist_ok=True)
    alpha = Project.alpha
    beta = Project.beta
    results: dict[str, int] = {}

    @alpha
    def work_alpha() -> None:
        alpha.t.create_or_replace_from(pl.DataFrame({"id": [100]}))
        results["alpha"] = alpha.t.read().height

    @beta
    def work_beta() -> None:
        beta.t.create_or_replace_from(pl.DataFrame({"id": [200, 201]}))
        results["beta"] = beta.t.read().height

    work_alpha()
    work_beta()

    assert results["alpha"] == 1
    assert results["beta"] == 2
    # Both should be disconnected
    assert not alpha.is_connected
    assert not beta.is_connected


def test_db_manual_connect_close(tmp_path: Path) -> None:
    """Manual connect/close methods work correctly."""

    class S(fl.Schema):
        v = fl.Int64()

    class MyDB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)
    db = Project.db

    assert not db.is_connected
    db.connect()
    assert db.is_connected
    db.t.create_or_replace_from(pl.DataFrame({"v": [42]}))
    assert db.t.read().get_column("v").to_list() == [42]
    db.close()
    assert not db.is_connected
