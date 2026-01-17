from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, NamedTuple, Self

import pyochain as pc

if TYPE_CHECKING:
    from .._columns import Column


class OnConflictResult(NamedTuple):
    update_clause: str
    conflict_target: str

    @classmethod
    def from_keys(
        cls, conflict_keys: pc.Set[str], schema: pc.Dict[str, Column]
    ) -> Self:
        """Obtains the conflict keys for the schema.

        Args:
            conflict_keys (pc.Set[str]): The conflict keys columns.
            schema (pc.Dict[str, Column]): The schema columns dictionary.

        Returns:
            Self: The conflict keys, prioritizing primary keys over unique keys.
        """
        target: str = conflict_keys.iter().map(lambda k: f'"{k}"').join(", ")

        update_clause: str = (
            schema.iter()
            .filter(lambda k: k not in conflict_keys)
            .map(lambda col: f'"{col}" = excluded."{col}"')
            .join(", ")
        )
        return cls(f"({target})", update_clause)


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
        return (
            self.primary.then_some()
            .or_else(lambda: self.uniques.then_some())
            .ok_or("At least one constraint expected")
        )

    @classmethod
    def from_cols(cls, cols: pc.Set[Column]) -> pc.Option[Self]:
        def _constraint_type(
            predicate: Callable[[Column], bool],
        ) -> pc.Option[pc.Set[str]]:
            return (
                cols.iter()
                .filter(predicate)
                .map(lambda c: c.name)
                .collect(pc.Set)
                .then_some()
            )

        return (
            _constraint_type(lambda c: c.primary_key)
            .zip(_constraint_type(lambda c: c.unique))
            .map(lambda x: cls(*x))
        )
