from collections.abc import Callable
from enum import StrEnum
from typing import NamedTuple

import pyochain as pc

from .._columns import Column


class _KWord(StrEnum):
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"


def col_to_sql(col: Column) -> str:
    definition: str = f'"{col.name}" {col.sql_type}'
    if col.primary_key:
        definition += " "
        definition += _KWord.PRIMARY_KEY
    if col.unique:
        definition += " "
        definition += _KWord.UNIQUE
    return definition


def _constraint_type(
    cols: pc.Iter[Column], predicate: Callable[[Column], bool]
) -> pc.Option[pc.Seq[str]]:
    constraint_cols = cols.filter(predicate).map(lambda c: c.name).collect()
    match constraint_cols.count():
        case 0:
            return pc.NONE
        case _:
            return pc.Some(constraint_cols)


class KeysConstraints(NamedTuple):
    """
    Holds the optional keys constraints of a schema.

    Since a table is not required to have any constraints, both
    `primary` and `uniques` are optional.

    Consult `pyochain.Option` documentation for more information about handling Option types.
    """

    primary: pc.Option[pc.Seq[str]]
    """The primary key columns, if any."""
    uniques: pc.Option[pc.Seq[str]]
    """The unique key columns, if any."""


def _constraints_to_result(
    primary: pc.Option[pc.Seq[str]], uniques: pc.Option[pc.Seq[str]]
) -> pc.Option[KeysConstraints]:
    match primary.is_some(), uniques.is_some():
        case False, False:
            return pc.NONE
        case _:
            return pc.Some(KeysConstraints(primary=primary, uniques=uniques))


def cols_to_constraints(cols: pc.Iter[Column]) -> pc.Option[KeysConstraints]:
    return _constraints_to_result(
        cols.pipe(_constraint_type, lambda c: c.primary_key),
        cols.pipe(_constraint_type, lambda c: c.unique),
    )
