"""Casts, compatibility, composed types creations."""

from pathlib import Path

import pyochain as pc
import pytest

import framelib as fl


def test_instantiate_schema() -> None:
    """Schema cannot be instantiated directly."""
    with pytest.raises(TypeError):
        _ = fl.Schema()


def test_schema_cast_and_multi_column_pk(tmp_path: Path) -> None:
    """Verify behavior when a Schema declares multiple primary_key columns."""
    with pytest.raises(pc.ResultUnwrapError):  # noqa: PT012

        class S(fl.Schema):
            a = fl.Int64(primary_key=True)
            b = fl.Int64(primary_key=True)
            val = fl.String()

        class DB(fl.DataBase):
            t = fl.Table(schema=S)

        class Project(fl.Folder):  # pyright: ignore[reportUnusedClass] # type: ignore[[unused-ignore]]
            __source__ = Path(tmp_path)
            db = DB()


def test_schema_empty_allowed() -> None:
    """Empty Schema is allowed."""

    class EmptyS(fl.Schema):
        pass

    assert EmptyS.entries().length() == 0


def test_schema_inheritance_column_order_preservation() -> None:
    """Inherited schema preserves parent columns before child columns."""

    class ParentS(fl.Schema):
        parent_col1 = fl.Int64()
        parent_col2 = fl.String()

    class ChildS(ParentS):
        child_col = fl.Float64()

    col_names = ChildS.entries().keys().into(tuple)
    # Parent columns should come first
    assert col_names.index("parent_col1") < col_names.index("child_col")
    assert col_names.index("parent_col2") < col_names.index("child_col")


def test_schema_to_sql_simple() -> None:
    """Schema generates valid SQL for simple structure."""

    class SimpleS(fl.Schema):
        id = fl.Int64()
        name = fl.String()

    sql = SimpleS.to_sql()
    assert '"id"' in sql
    assert '"name"' in sql
