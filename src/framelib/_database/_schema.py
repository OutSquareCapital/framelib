from __future__ import annotations

from typing import overload

import narwhals as nw
import polars as pl
import pyochain as pc
from narwhals.typing import IntoLazyFrameT, LazyFrameT

from .._columns import Column
from .._core import BaseLayout
from ._constraints import KeysConstraints, cols_to_constraints


def _schema_from_mro(cls: type) -> dict[str, Column]:
    """
    Constructs the schema dictionary from the class MRO.
    - Create an Iterator over the MRO
    - Filter only Schema subclasses (excluding the parent Schema class itself)
    - Reverse the order to have the base classes first
    - For each base class, filter its attributes to keep only Column entries
    - Extract the items (name, Column) as tuples
    - Flatten the list of tuples into a single iterable
    Args:
        cls (type): The schema class.
    Returns:
        out (dict[str, Column]): The final schema as a dictionary.
    """
    return (
        pc.Iter.from_(cls.mro())
        .filter_subclass(Schema, keep_parent=False)
        .reverse()
        .map(
            lambda base: (
                pc.Dict.from_object(base).filter_type(Column).iter_items().inner()
            )
        )
        .flatten()
        .into(dict)
    )


class Schema(BaseLayout[Column]):
    """
    A schema is a layout containing only Column entries.

    Used to define the schema of a Table or a File.
    """

    _constraints: pc.Option[KeysConstraints]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls._schema = _schema_from_mro(cls)
        cls._constraints = cls.schema().iter_values().pipe(cols_to_constraints)
        return

    @classmethod
    def constraints(cls) -> pc.Option[KeysConstraints]:
        """
        Returns:
            out (pyochain.Option[KeysConstraints]): the keys constraints of the schema, if any.
        """
        return cls._constraints

    @classmethod
    def sql_schema(cls) -> str:
        """
        Returns:
            str: The SQL schema definition.
        """
        return cls.schema().iter_values().map(lambda col: col.to_sql()).join(", ")

    @classmethod
    def pl_schema(cls) -> pl.Schema:
        """
        Syntactic sugar for getting the Polars schema definition.

        Equivalent to: `Foo.schema().map_values(lambda c: c.pl_dtype).into(pl.Schema)`

        Returns:
            out (polars.Schema): The Polars schema definition.
        """
        return cls.schema().map_values(lambda c: c.pl_dtype).into(pl.Schema)

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
        cls,
        df: IntoLazyFrameT | LazyFrameT | pl.DataFrame,
    ) -> LazyFrameT | nw.LazyFrame[IntoLazyFrameT]:
        """
        - Selects only the columns defined in the schema
        - Casts them to the correct dtype
        - Returns the results wrapped in a narwhals `LazyFrame`.

        Use `.to_native()` to convert back to the native DataFrame type.

        Args:
            df (IntoLazyFrameT | LazyFrameT | pl.DataFrame): The input DataFrame.
        Returns:
            out (LazyFrameT | nw.LazyFrame[IntoLazyFrameT]): The casted narwhals.LazyFrame.

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
                cls.schema()
                .iter_values()
                .map(lambda col: col.nw_col.cast(col.nw_dtype))
                .inner()
            )
        )

    @classmethod
    def cast_strict_false(cls, df: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame:
        """
        Like `cast`, but with `strict=False`.

        **Warning**:
            will only work with polars {Data, Lazy}Frames.

        Args:
            df (pl.LazyFrame | pl.DataFrame): The input DataFrame.
        Returns:
            out (polars.LazyFrame): The casted `polars.LazyFrame`.
        """
        return df.lazy().select(
            cls.schema()
            .iter_values()
            .map(lambda c: c.pl_col.cast(c.pl_dtype, strict=False))
            .inner()
        )
