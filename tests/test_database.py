"""Tests for DataBase connection decorator usage within a Folder."""

from pathlib import Path

import duckdb
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
        users = fl.Table(schema=UserSchema)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    # ensure folder exists so DuckDB can create the file
    Project.source().mkdir(parents=True, exist_ok=True)

    @Project.db
    def fn_simple() -> None:
        assert Project.db.is_connected
        assert Project.db.connexion is not None
        assert Project.db.users.connexion.unwrap() is Project.db.connexion

    fn_simple()
    assert not Project.db.is_connected


def test_nested_and_multiple_db_decorators(tmp_path: Path) -> None:
    """Multiple DBs as Folder entries: nested and stacked decorators."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)

    class DBA(fl.DataBase):
        t = fl.Table(schema=S)

    class DBB(fl.DataBase):
        t = fl.Table(schema=S)

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
        t = fl.Table(schema=DerivedS)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        assert Project.db.is_connected
        # table should have received the same connexion instance
        assert Project.db.t.connexion.unwrap() is Project.db.connexion

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
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        first_conn = Project.db.connexion
        assert Project.db.is_connected
        # Nested enter should keep the same connection
        with Project.db:
            assert Project.db.is_connected
            assert Project.db.connexion is first_conn
        # After nested exit, should still be connected with same conn
        assert Project.db.is_connected
        assert Project.db.connexion is first_conn

    assert not Project.db.is_connected


def test_db_decorator_chained_calls_same_db(tmp_path: Path) -> None:
    """Multiple decorated functions using same DB share connection state correctly."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        value = fl.String()

    class MyDB(fl.DataBase):
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    call_order: list[str] = []

    @Project.db
    def first_op() -> None:
        call_order.append("first_start")
        Project.db.t.create_or_replace().insert_into(
            pl.DataFrame({"id": [1], "value": ["a"]})
        )
        second_op()
        call_order.append("first_end")

    @Project.db
    def second_op() -> None:
        call_order.append("second_start")
        assert Project.db.t.read().height == 1
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
        users = fl.Table(schema=UserSchema)
        orders = fl.Table(schema=OrderSchema)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        users_df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        orders_df = pl.DataFrame(
            {
                "id": [10, 20, 30],
                "user_id": [1, 1, 2],
                "product": ["Widget", "Gadget", "Thing"],
            }
        )

        # Both tables accessible
        assert (
            Project.db.users.create_or_replace().insert_into(users_df).read().height
            == 2
        )
        assert (
            Project.db.orders.create_or_replace().insert_into(orders_df).read().height
            == 3
        )


def test_db_connection_error_propagation(tmp_path: Path) -> None:
    """Errors in decorated functions close connection properly."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)

    class MyDB(fl.DataBase):
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    @Project.db
    def failing_func() -> None:
        Project.db.t.create_or_replace().insert_into(pl.DataFrame({"id": [1]}))
        msg = "Intentional error"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="Intentional error"):
        failing_func()

    # Connection should be closed after exception
    assert not Project.db.is_connected


def test_db_sql_execution_within_context(tmp_path: Path) -> None:
    """Raw SQL queries can be executed within connection context."""

    class S(fl.Schema):
        x = fl.Int64()
        y = fl.Float64()

    class MyDB(fl.DataBase):
        data = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        df = pl.DataFrame({"x": [1, 2, 3], "y": [1.5, 2.5, 3.5]})
        Project.db.data.create_or_replace().insert_into(df)
        qry = "SELECT SUM(x) as total_x, AVG(y) as avg_y FROM data"
        result = Project.db.sql(qry).collect()
        assert result.get_column("total_x").to_list()[0] == 6
        assert result.get_column("avg_y").to_list()[0] == pytest.approx(2.5)  # pyright: ignore[reportUnknownMemberType]


def test_db_show_tables_reflects_schema(tmp_path: Path) -> None:
    """show_tables reflects all tables created from schema."""

    class S1(fl.Schema):
        a = fl.Int64()

    class S2(fl.Schema):
        b = fl.String()

    class MyDB(fl.DataBase):
        table_one = fl.Table(schema=S1)
        table_two = fl.Table(schema=S2)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        Project.db.table_one.create_or_replace().insert_into(pl.DataFrame({"a": [1]}))
        Project.db.table_two.create_or_replace().insert_into(pl.DataFrame({"b": ["x"]}))
        tables = Project.db.show_tables().collect()
        table_names = pc.Set[str](tables.get_column("name"))
        assert "table_one" in table_names
        assert "table_two" in table_names


def test_db_concurrent_decorated_functions_different_dbs(tmp_path: Path) -> None:
    """Different databases in same folder can be used independently."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)

    class DBA(fl.DataBase):
        t = fl.Table(schema=S)

    class DBB(fl.DataBase):
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        alpha = DBA()
        beta = DBB()

    Project.source().mkdir(parents=True, exist_ok=True)
    results = pc.Dict[str, int].new()

    @Project.alpha
    def work_alpha() -> None:
        df = pl.DataFrame({"id": [100]})
        results.insert(
            "alpha", Project.alpha.t.create_or_replace().insert_into(df).read().height
        )

    @Project.beta
    def work_beta() -> None:
        df = pl.DataFrame({"id": [200, 201]})

        results.insert(
            "beta", Project.beta.t.create_or_replace().insert_into(df).read().height
        )

    work_alpha()
    work_beta()

    assert results.get_item("alpha").unwrap() == 1
    assert results.get_item("beta").unwrap() == 2
    # Both should be disconnected
    assert not Project.alpha.is_connected
    assert not Project.beta.is_connected


def test_db_manual_connect_close(tmp_path: Path) -> None:
    """Manual connect/close methods work correctly."""

    class S(fl.Schema):
        v = fl.Int64()

    class MyDB(fl.DataBase):
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    assert not Project.db.is_connected
    assert Project.db.connect().is_connected
    assert Project.db.t.create_or_replace().insert_into(
        pl.DataFrame({"v": [42]})
    ).read().get_column("v").to_list() == [42]

    assert not Project.db.close().is_connected


def test_schema_composite_pk_duckdb_integration(tmp_path: Path) -> None:
    """Composite PK should work correctly with DuckDB insert_or_replace."""

    class S(fl.Schema):
        a = fl.Int64(primary_key=True)
        b = fl.Int64(primary_key=True)
        val = fl.String()

    class DB(fl.DataBase):
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = tmp_path
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    df1 = pl.DataFrame({"a": [1, 1, 2], "b": [1, 2, 1], "val": ["x", "y", "z"]})

    with Project.db:
        Project.db.t.create_or_replace().insert_into(df1)

        # Update (1, 2) -> should only update that specific composite key
        df2 = pl.DataFrame({"a": [1], "b": [2], "val": ["updated"]})
        Project.db.t.insert_or_replace(df2)

        result = Project.db.t.read().sort("a", "b")
        assert result.shape == (3, 3)
        # (1,1) unchanged, (1,2) updated, (2,1) unchanged
        vals = result.get_column("val").to_list()
        assert vals == ["x", "updated", "z"]


def test_unique_constraint_duckdb_integration(tmp_path: Path) -> None:
    """UNIQUE constraint should be enforced by DuckDB."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        email = fl.String(unique=True)

    class DB(fl.DataBase):
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = tmp_path
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    df1 = pl.DataFrame({"id": [1, 2], "email": ["a@x.com", "b@x.com"]})

    with Project.db:
        Project.db.t.create_or_replace().insert_into(df1)

        # Try to insert duplicate email (different id) - should fail
        df2 = pl.DataFrame({"id": [3], "email": ["a@x.com"]})
        with pytest.raises(duckdb.ConstraintException):
            Project.db.t.insert_into(df2)


def test_not_null_constraint_duckdb_integration(tmp_path: Path) -> None:
    """NOT NULL constraint should be enforced by DuckDB."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        name = fl.String(nullable=False)

    class DB(fl.DataBase):
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = tmp_path
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        Project.db.t.create_or_replace()

        # Try to insert NULL in non-nullable column - should fail
        df = pl.DataFrame({"id": [1], "name": [None]})
        with pytest.raises(duckdb.ConstraintException):
            Project.db.t.insert_into(df)


def test_composite_unique_duckdb_integration(tmp_path: Path) -> None:
    """Composite UNIQUE constraint should be enforced by DuckDB."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        a = fl.Int64(unique=True)
        b = fl.Int64(unique=True)

    class DB(fl.DataBase):
        t = fl.Table(schema=S)

    class Project(fl.Folder):
        __source__ = tmp_path
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    # Insert (a=1, b=1) and (a=1, b=2) - should work (different composite key)
    df1 = pl.DataFrame({"id": [1, 2], "a": [1, 1], "b": [1, 2]})

    with Project.db:
        Project.db.t.create_or_replace().insert_into(df1)

        # Try to insert duplicate (a=1, b=1) - should fail
        df2 = pl.DataFrame({"id": [3], "a": [1], "b": [1]})
        with pytest.raises(duckdb.ConstraintException):
            Project.db.t.insert_into(df2)
