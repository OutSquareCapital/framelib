from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple, Self

import pyochain as pc

if TYPE_CHECKING:
    from .._columns import Column


class PrimaryKeyError(RuntimeError):
    """Raised when multiple primary key columns are defined in a schema."""


class KeysConstraints(NamedTuple):
    """Holds the optional keys constraints of a schema.

    Since a table is not required to have any constraints, both
    `primary` and `uniques` are optional.

    Consult `pyochain.Option` documentation for more information about handling Option types.
    """

    primary: pc.Option[str]
    """The primary key column, if any."""
    uniques: pc.Option[pc.Set[str]]
    """The unique key columns, if any."""

    @classmethod
    def from_cols(cls, cols: pc.Set[Column]) -> pc.Result[Self, PrimaryKeyError]:
        primaries = cols.iter().filter(lambda c: c.primary_key).collect()
        if primaries.length() > 1:
            msg = f"Multiple primary key columns detected\n: {primaries}"
            return pc.Err(PrimaryKeyError(msg))
        uniques = (
            cols.iter()
            .filter(lambda c: c.unique)
            .map(lambda c: c.name)
            .collect(pc.Set)
            .then_some()
        )
        return pc.Ok(cls(primaries.then_some().map(lambda p: p.first().name), uniques))
