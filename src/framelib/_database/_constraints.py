from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum

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


class ConstraintsError(ValueError):
    """Raised when there is an error with schema constraints."""

    def __init__(self, constraint_keys: Iterable[str], name: str) -> None:
        super().__init__(
            f"Ambiguous unique constraints for table '{name}': "
            f"{constraint_keys}. "
            "Define a primary_key or a single unique column to use "
            "`insert_or_replace` upsert semantics."
        )


def _constraint_type(
    cols: pc.Iter[Column], predicate: Callable[[Column], bool]
) -> pc.Option[pc.Seq[str]]:
    constraint_cols = cols.filter(predicate).map(lambda c: c.name).collect()
    match constraint_cols.count():
        case 0:
            return pc.NONE
        case _:
            return pc.Some(constraint_cols)


@dataclass(slots=True, init=False)
class KeysConstraints:
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

    def __init__(self, cols: pc.Iter[Column]) -> None:
        self.primary = _constraint_type(cols, lambda c: c.primary_key)
        self.uniques = _constraint_type(cols, lambda c: c.unique)

    @property
    def all(self) -> pc.Option[pc.Seq[str]]:
        """Returns all unique keys (primary + uniques) as a single Option."""

        return self.primary.zip_with(
            self.uniques, lambda p, u: p.iter().chain(u.iter().inner()).collect()
        )

    def on_conflict(self, name: str) -> pc.Result[Sequence[str], ValueError]:
        return (
            self.primary.map(lambda c: c.inner())
            .or_else(lambda: self.uniques.map(lambda u: u.inner()))
            .ok_or(
                self.all.expect("No unique constraints found").into(
                    ConstraintsError,
                    name,
                )
            )
        )
