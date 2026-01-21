from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import TYPE_CHECKING, NamedTuple, Self

import pyochain as pc

if TYPE_CHECKING:
    from .._columns import Column


class KWord(StrEnum):
    """Represents SQL keywords for constraints."""

    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT NULL"


class Constraint(NamedTuple):
    """Represents a constraint on a set of columns."""

    cols: pc.Set[Column]
    k_word: KWord

    @classmethod
    def new(
        cls, cols: pc.Set[Column], predicate: Callable[[Column], bool], k_word: KWord
    ) -> pc.Option[Self]:
        return (
            cols.iter()
            .filter(predicate)
            .collect(pc.Set)
            .then_some()
            .map(lambda cs: cls(cs, k_word))
        )

    def is_composite(self) -> bool:
        return self.cols.length() > 1

    def to_sql(self) -> str:
        return f"{self.k_word} ({self.cols.iter().map(lambda c: f'"{c.name}"').join(', ')})"


class KeysConstraints(NamedTuple):
    """Holds the various constraints of a schema.

    Since a table is not required to have any constraints, all fields are optional.

    Consult `pyochain.Option` documentation for more information about handling Option types.
    """

    primary: pc.Option[Constraint]
    """The primary key column(s), if any. Can be composite (multiple columns)."""
    uniques: pc.Option[Constraint]
    """The unique key columns, if any. Can be composite (multiple columns)."""
    not_nulls: pc.Option[Constraint]
    """The NOT NULL columns, if any."""

    @classmethod
    def from_cols(cls, cols: pc.Set[Column]) -> Self:
        """Build constraints from a set of columns.

        Args:
            cols (pc.Set[Column]): The columns to extract constraints from.

        Returns:
            Self: The constructed KeysConstraints.
        """
        return cls(
            Constraint.new(cols, lambda c: c.primary_key, KWord.PRIMARY_KEY),
            Constraint.new(cols, lambda c: c.unique, KWord.UNIQUE),
            Constraint.new(cols, lambda c: not c.nullable, KWord.NOT_NULL),
        )
