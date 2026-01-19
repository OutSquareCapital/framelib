"""Narhwals syntax and CRUD operations tests."""

from pathlib import Path

import duckdb
import narwhals as nw
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

    df1 = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    with Project.db:
        # create or replace

        res = Project.db.t.create_or_replace().insert_into(df1).read()
        assert res.get_column("id").to_list() == [1, 2]
        assert res.get_column("name").to_list() == ["a", "b"]

        # insert_or_replace: update id=2 and add id=3
        df2 = pl.DataFrame({"id": [2, 3], "name": ["bb", "c"]})
        res2 = Project.db.t.insert_or_replace(df2).read().sort("id")
        updated_id = 2
        assert res2.get_column("id").to_list() == [1, 2, 3]
        assert res2.filter(pl.col("id") == updated_id).get_column("name").to_list() == [
            "bb"
        ]

        # insert_or_ignore: try to insert duplicate id=3 (should be ignored)
        df3 = pl.DataFrame({"id": [3], "name": ["ccc"]})

        ignored_id = 3
        assert Project.db.t.insert_or_ignore(df3).read().sort("id").filter(
            pl.col("id") == ignored_id
        ).get_column("name").to_list() == ["c"]

        # truncate
        assert Project.db.t.truncate().read().height == 0

        # recreate then drop
        with pytest.raises(duckdb.CatalogException):
            _ = Project.db.t.create_or_replace().insert_into(df1).drop().relation


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
        _ = Project.db.t.relation.unwrap()


# ============================================================================
# Complex CRUD Operations Tests
# ============================================================================


def test_table_insert_into_append_behavior(tmp_path: Path) -> None:
    """insert_into appends rows without checking conflicts."""

    class S(fl.Schema):
        id = fl.Int64()
        value = fl.String()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        Project.db.t.create_or_replace().insert_into(
            pl.DataFrame({"id": [1], "value": ["a"]})
        )
        # Insert more rows
        Project.db.t.insert_into(pl.DataFrame({"id": [2, 3], "value": ["b", "c"]}))
        result = Project.db.t.read().sort("id")
        assert result.height == 3
        assert result.get_column("value").to_list() == ["a", "b", "c"]


def test_table_bulk_insert_or_replace(tmp_path: Path) -> None:
    """insert_or_replace handles bulk updates efficiently."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        counter = fl.Int64()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        # Initial bulk insert
        initial_df = pl.DataFrame(
            {
                "id": pc.Iter(range(100)).collect(),
                "counter": pc.Iter(range(100)).map(lambda _: 0).collect(),
            }
        )
        Project.db.t.create_or_replace().insert_into(initial_df)

        # Update half of them
        update_df = pl.DataFrame(
            {
                "id": pc.Iter(range(0, 100, 2)).collect(),
                "counter": pc.Iter(range(50)).map(lambda _: 1).collect(),
            }
        )

        result = Project.db.t.insert_or_replace(update_df).read()

        assert result.height == 100
        assert result.filter(pl.col("counter") == 1).height == 50


def test_table_chain_operations(tmp_path: Path) -> None:
    """Multiple table operations can be chained fluently."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        status = fl.String()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        statuses = (
            Project.db.t.create_or_replace()
            .insert_into(
                pl.DataFrame({"id": [1, 2, 3], "status": ["new", "new", "new"]})
            )
            .insert_or_replace(pl.DataFrame({"id": [2], "status": ["updated"]}))
            .insert_or_ignore(pl.DataFrame({"id": [3], "status": ["ignored"]}))
            .read()
            .sort("id")
            .get_column("status")
            .to_list()
        )

        assert statuses[0] == "new"  # id=1 unchanged
        assert statuses[1] == "updated"  # id=2 replaced
        assert statuses[2] == "new"  # id=3 ignored


def test_table_scan_narwhals_operations(tmp_path: Path) -> None:
    """Table scan returns DuckFrame supporting narwhals operations."""

    class S(fl.Schema):
        category = fl.String()
        amount = fl.Float64()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        df = pl.DataFrame(
            {
                "category": ["A", "B", "A", "B", "A"],
                "amount": [100.0, 200.0, 150.0, 250.0, 175.0],
            }
        )

        # Use scan for lazy narwhals operations

        lf = Project.db.t.create_or_replace().insert_into(df).scan()
        assert isinstance(lf, nw.LazyFrame)

        # Aggregate using narwhals
        totals = (
            lf.group_by("category")
            .agg(fl.col("amount").sum().alias("total"))
            .sort("category")
            .collect()
            .pipe(lambda df: pc.Iter(df.iter_rows()))
            .map_star(lambda cat, total: (cat, total))
            .collect(pc.Dict)
        )
        assert totals.get_item("A").unwrap() == 425.0
        assert totals.get_item("B").unwrap() == 450.0


def test_table_summarize(tmp_path: Path) -> None:
    """Table summarize provides column statistics."""

    class S(fl.Schema):
        value = fl.Float64()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        assert (
            Project.db.t.create_or_replace()
            .insert_into(pl.DataFrame({"value": [1.0, 2.0, 3.0, 4.0, 5.0]}))
            .summarize()
            .to_native()
            .pl()
            .height
            > 0
        )


def test_table_describe_columns(tmp_path: Path) -> None:
    """Table describe_columns provides schema information."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        name = fl.String()
        score = fl.Float64()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        col_names = pc.Set[str](
            Project.db.t.create_or_replace()
            .insert_into(pl.DataFrame({"id": [1], "name": ["test"], "score": [99.5]}))
            .describe_columns()
            .collect()
            .get_column("column_name")
        )
        assert "id" in col_names
        assert "name" in col_names
        assert "score" in col_names


def test_table_multiple_primary_key_conflict_handling(tmp_path: Path) -> None:
    """Primary key conflicts are handled correctly across operations."""

    class S(fl.Schema):
        pk = fl.Int64(primary_key=True)
        data = fl.String()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        result = (
            Project.db.t.create_or_replace()
            .insert_into(pl.DataFrame({"pk": [1, 2, 3], "data": ["a", "b", "c"]}))
            .insert_or_replace(pl.DataFrame({"pk": [1, 4], "data": ["A", "d"]}))
            .insert_or_ignore(pl.DataFrame({"pk": [2, 5], "data": ["B", "e"]}))
            .read()
            .sort("pk")
            .iter_rows()
        )

        data_dict = pc.Dict[int, str](
            pc.Iter(result).map_star(lambda pk, data: (pk, data)).collect()
        )
        assert data_dict.get_item(1).unwrap() == "A"  # replaced
        assert data_dict.get_item(2).unwrap() == "b"  # ignored (kept original)
        assert data_dict.get_item(3).unwrap() == "c"  # unchanged
        assert data_dict.get_item(4).unwrap() == "d"  # new
        assert data_dict.get_item(5).unwrap() == "e"  # new


def test_table_truncate_preserves_schema(tmp_path: Path) -> None:
    """Truncate removes all rows but preserves table schema."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        value = fl.String()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        assert (
            Project.db.t.create_or_replace()
            .insert_into(pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]}))
            .truncate()
            .read()
            .height
            == 0
        )

        # Can still insert new data

        assert (
            Project.db.t.insert_into(pl.DataFrame({"id": [10], "value": ["new"]}))
            .read()
            .height
            == 1
        )


def test_table_create_from_fails_if_exists(tmp_path: Path) -> None:
    """create_from raises error if table already exists."""

    class S(fl.Schema):
        v = fl.Int64()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        Project.db.t.create().insert_into(pl.DataFrame({"v": [1]}))
        with pytest.raises(duckdb.CatalogException):
            Project.db.t.create().insert_into(pl.DataFrame({"v": [2]}))


def test_table_large_dataset_operations(tmp_path: Path) -> None:
    """Table handles large datasets efficiently."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        category = fl.String()
        value = fl.Float64()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    n_rows = 10000

    with Project.db:
        df = pl.DataFrame(
            {
                "id": pc.Iter(range(n_rows)).collect(),
                "category": pc.Iter(range(n_rows))
                .map(lambda i: f"cat_{i % 10}")
                .collect(),
                "value": pc.Iter(range(n_rows)).map(float).collect(),
            }
        )
        assert Project.db.t.create_or_replace().insert_into(df).read().height == n_rows

        assert (
            Project.db.t.scan()
            .group_by("category")
            .agg(fl.col("value").sum().alias("total"))
            .to_native()
            .pl()
            .height
            == 10
        )  # 10 unique categories


def test_table_create(tmp_path: Path) -> None:
    """Test create table if not exists operation."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        name = fl.String()

    class DB(fl.DataBase):
        t = fl.Table(model=S)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        db = DB()

    Project.source().mkdir(parents=True, exist_ok=True)

    with Project.db:
        # First creation
        result = Project.db.t.create().read()
        assert result.height == 0  # Table should be empty after creation
        assert result.width == 2  # Two columns

        # Second creation should fail
        with pytest.raises(duckdb.CatalogException):
            Project.db.t.create().read()
        # Should not raise error
        assert Project.db.t.create_if_not_exist().read().height == 0
        # Should recreate table
        assert Project.db.t.drop().create_if_not_exist().read().height == 0

        # Should replace existing table
        assert (
            Project.db.t.insert_into(pl.DataFrame({"id": [1], "name": ["test"]}))
            .create_if_not_exist()
            .read()
            .height
            == 1
        )
        assert Project.db.t.create_or_replace().read().height == 0
