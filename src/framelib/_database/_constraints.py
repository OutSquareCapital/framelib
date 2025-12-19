from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, NamedTuple

import pyochain as pc

if TYPE_CHECKING:
    from .._columns import Column


def _constraint_type(
    cols: pc.Seq[Column],
    predicate: Callable[[Column], bool],
) -> pc.Option[pc.Seq[str]]:
    constraint_cols = cols.iter().filter(predicate).map(lambda c: c.name).collect()
    match constraint_cols.count():
        case 0:
            return pc.NONE
        case _:
            return pc.Some(constraint_cols)


class OnConflictResult(NamedTuple):
    update_clause: str
    conflict_target: str


@dataclass(slots=True)
class KeysConstraints:
    """Holds the optional keys constraints of a schema.

    Since a table is not required to have any constraints, both
    `primary` and `uniques` are optional.

    Consult `pyochain.Option` documentation for more information about handling Option types.

    Attributes:
        primary (pc.Option[pc.Seq[str]]): The primary key columns, if any.
        uniques (pc.Option[pc.Seq[str]]): The unique key columns, if any.
    """

    primary: pc.Option[pc.Seq[str]]
    """The primary key columns, if any."""
    uniques: pc.Option[pc.Seq[str]]
    """The unique key columns, if any."""

    @property
    def conflict_keys(self) -> pc.Seq[str]:
        msg = "At least one constraint expected"
        return self.primary.unwrap_or(self.uniques.expect(msg))


def on_conflict(
    conflict_keys: pc.Seq[str],
    schema: pc.Dict[str, Column],
) -> OnConflictResult:
    """Obtains the conflict keys for the schema.

    Args:
        conflict_keys (pc.Seq[str]): The conflict keys columns.
        schema (pc.Dict[str, Column]): The schema columns dictionary.

    Returns:
        OnConflictResult: The conflict keys, prioritizing primary keys over unique keys.
    """
    target: str = conflict_keys.iter().map(lambda k: f'"{k}"').join(", ")

    update_clause: str = (
        schema.iter_keys()
        .filter(lambda k: k not in conflict_keys.inner())
        .map(lambda col: f'"{col}" = excluded."{col}"')
        .join(", ")
    )
    return OnConflictResult(f"({target})", update_clause)


def cols_to_constraints(cols: pc.Seq[Column]) -> pc.Option[KeysConstraints]:
    primary: pc.Option[pc.Seq[str]] = cols.pipe(
        _constraint_type, lambda c: c.primary_key
    )
    uniques: pc.Option[pc.Seq[str]] = cols.pipe(_constraint_type, lambda c: c.unique)
    match primary.is_some(), uniques.is_some():
        case False, False:
            return pc.NONE
        case _:
            return pc.Some(KeysConstraints(primary=primary, uniques=uniques))
