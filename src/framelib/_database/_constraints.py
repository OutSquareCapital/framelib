from collections.abc import Callable
from typing import TYPE_CHECKING, NamedTuple

import pyochain as pc

if TYPE_CHECKING:
    from .._columns import Column


class OnConflictResult(NamedTuple):
    update_clause: str
    conflict_target: str


class KeysConstraints(NamedTuple):
    """Holds the optional keys constraints of a schema.

    Since a table is not required to have any constraints, both
    `primary` and `uniques` are optional.

    Consult `pyochain.Option` documentation for more information about handling Option types.

    Attributes:
        primary (pc.Set[str]): The primary key columns, if any.
        uniques (pc.Set[str]): The unique key columns, if any.
    """

    primary: pc.Set[str]
    """The primary key columns, if any."""
    uniques: pc.Set[str]
    """The unique key columns, if any."""

    @property
    def conflict_keys(self) -> pc.Result[pc.Set[str], str]:
        if self.primary.any():
            return pc.Ok(self.primary)
        if self.uniques.any():
            return pc.Ok(self.uniques)
        return pc.Err("At least one constraint expected")


def on_conflict(
    conflict_keys: pc.Set[str],
    schema: pc.Dict[str, Column],
) -> OnConflictResult:
    """Obtains the conflict keys for the schema.

    Args:
        conflict_keys (pc.Set[str]): The conflict keys columns.
        schema (pc.Dict[str, Column]): The schema columns dictionary.

    Returns:
        OnConflictResult: The conflict keys, prioritizing primary keys over unique keys.
    """
    target: str = conflict_keys.iter().map(lambda k: f'"{k}"').join(", ")

    update_clause: str = (
        schema.keys_iter()
        .filter(lambda k: k not in conflict_keys)
        .map(lambda col: f'"{col}" = excluded."{col}"')
        .join(", ")
    )
    return OnConflictResult(f"({target})", update_clause)


def cols_to_constraints(cols: pc.Set[Column]) -> pc.Option[KeysConstraints]:
    def _constraint_type(predicate: Callable[[Column], bool]) -> pc.Option[pc.Set[str]]:
        constraint_cols = (
            cols.iter().filter(predicate).map(lambda c: c.name).collect(pc.Set)
        )
        match constraint_cols.any():
            case False:
                return pc.NONE
            case True:
                return pc.Some(constraint_cols)

    return (
        _constraint_type(lambda c: c.primary_key)
        .zip(_constraint_type(lambda c: c.unique))
        .map(lambda x: KeysConstraints(*x))
    )
