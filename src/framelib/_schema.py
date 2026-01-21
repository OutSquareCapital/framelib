from __future__ import annotations

from typing import TYPE_CHECKING, TypeIs, overload

import narwhals as nw
import polars as pl
import pyochain as pc

from ._columns import Column
from ._core import Layout
from ._database._constraints import KeysConstraints, KWord

if TYPE_CHECKING:
    from narwhals.typing import IntoLazyFrameT, LazyFrameT


class Schema(Layout[Column]):
    """A schema is a layout containing only Column entries.

    Used to define the schema of a Table or a File.
    """

    _constraints: KeysConstraints

    def __new__(cls) -> None:
        msg = "Schema cannot be instantiated directly."
        raise TypeError(msg)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls._entries = _entries_from_mro(cls)
        cls._constraints = (
            cls.entries()
            .values()
            .iter()
            .collect(pc.Set)
            .into(KeysConstraints.from_cols)
        )

    @classmethod
    def constraints(cls) -> KeysConstraints:
        """Get the eventual keys constraints of the schema.

        Returns:
            KeysConstraints: the keys constraints of the schema, if any.
        """
        return cls._constraints

    @classmethod
    def to_sql(cls) -> str:
        """Get the SQL schema definition."""
        composite_pk = cls._constraints.primary.filter(lambda pk: pk.is_composite())
        composite_unique = cls._constraints.uniques.filter(lambda u: u.is_composite())

        def _col_sql(col: Column) -> str:
            parts = pc.Vec([col.sql_col])
            if col.primary_key and composite_pk.is_none():
                parts.append(KWord.PRIMARY_KEY)
            if col.unique and composite_unique.is_none():
                parts.append(KWord.UNIQUE)
            if not col.nullable:
                parts.append(KWord.NOT_NULL)
            return parts.join(" ")

        table_constraints = (
            pc.Iter((composite_pk, composite_unique))
            .filter(lambda constr: constr.is_some())
            .map(lambda constr: constr.unwrap().to_sql())
        )
        return (
            cls.entries()
            .values()
            .iter()
            .map(_col_sql)
            .chain(table_constraints)
            .join(", ")
        )

    @classmethod
    def to_pl(cls) -> pl.Schema:
        """Get the schema as a Polars schema.

        Returns:
            pl.Schema: The Polars schema definition.
        """
        return (
            cls.entries()
            .items()
            .iter()
            .map_star(lambda name, c: (name, c.pl_dtype))
            .collect(pl.Schema)
        )

    @overload
    @classmethod
    def cast(cls, df: LazyFrameT) -> LazyFrameT: ...

    @overload
    @classmethod
    def cast(cls, df: IntoLazyFrameT) -> nw.LazyFrame[IntoLazyFrameT]: ...

    @overload
    @classmethod
    def cast(cls, df: pl.DataFrame) -> nw.LazyFrame[pl.LazyFrame]: ...

    @classmethod
    def cast(
        cls, df: IntoLazyFrameT | LazyFrameT | pl.DataFrame
    ) -> LazyFrameT | nw.LazyFrame[IntoLazyFrameT]:
        """Casts the input DataFrame to match the schema.

        Steps:
            - Selects only the columns defined in the schema
            - Casts them to the correct dtype
            - Returns the results wrapped in a narwhals `LazyFrame`.

        Use `.to_native()` to convert back to the native DataFrame type.

        Args:
            df (IntoLazyFrameT | LazyFrameT | pl.DataFrame): The input DataFrame.

        Returns:
            LazyFrameT | nw.LazyFrame[IntoLazyFrameT]: The casted narwhals.LazyFrame.

        Examples:
        ```python
        >>> import framelib as fl
        >>> import polars as pl
        >>>
        >>> class MySchema(Schema):
        ...    id = fl.Int32(primary_key=True)
        ...    name = fl.String()
        ...    age = fl.Int8()
        >>> df = pl.DataFrame({
        ...    "id": [1, 2, 3],
        ...    "name": ["Alice", "Bob", "Charlie"],
        ...    "age": [30, 25, 35],
        ...    "extra_col": ["extra1", "extra2", "extra3"]
        ... })
        >>>
        >>> df.pipe(MySchema.cast).to_native().collect().pipe(print)
        shape: (3, 3)
        ┌─────┬─────────┬─────┐
        │ id  ┆ name    ┆ age │
        │ --- ┆ ---     ┆ --- │
        │ i32 ┆ str     ┆ i8  │
        ╞═════╪═════════╪═════╡
        │ 1   ┆ Alice   ┆ 30  │
        │ 2   ┆ Bob     ┆ 25  │
        │ 3   ┆ Charlie ┆ 35  │
        └─────┴─────────┴─────┘

        ```

        """
        return (
            nw.from_native(df)
            .lazy()
            .select(
                cls.entries()
                .values()
                .iter()
                .map(lambda col: col.nw_col.cast(col.nw_dtype))
            )
        )

    @classmethod
    def cast_strict_false(cls, df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        """Like `cast`, but with `strict=False`.

        **Warning**:
            will only work with polars `{Data, Lazy}Frames`.

        Args:
            df (pl.LazyFrame | pl.DataFrame): The input DataFrame.

        Returns:
            pl.LazyFrame: The casted `polars.LazyFrame`.
        """
        return df.lazy().select(
            cls.entries()
            .values()
            .iter()
            .map(lambda c: c.pl_col.cast(c.pl_dtype, strict=False))
        )


def _entries_from_mro(cls: type) -> pc.Dict[str, Column]:
    """Constructs the entries dictionary from the class MRO.

    Steps:
        - Create an Iterator over the MRO
        - Filter only Schema subclasses (excluding the parent Schema class itself)
        - Reverse the order to have the base classes first
        - For each base class, filter its attributes to keep only Column entries
        - Extract the items (name, Column) as tuples
        - Flatten the list of tuples into a single iterable

    Args:
        cls (type): The schema class.

    Returns:
        pc.Dict[str, Column]: The final entries as a dictionary.
    """

    def _is_subclass_of_schema(c: type) -> TypeIs[type[Schema]]:
        return issubclass(c, Schema) and c is not Schema

    def _is_column(v: object) -> TypeIs[Column]:
        return isinstance(v, Column)

    return (
        pc.Iter(cls.mro())
        .filter(_is_subclass_of_schema)
        .collect()
        .rev()
        .flat_map(lambda base: base.__dict__.items())
        .filter_star(lambda _, v: _is_column(v))
        .collect(pc.Dict)
    )
