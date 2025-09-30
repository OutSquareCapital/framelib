from __future__ import annotations

from typing import Self

import narwhals as nw
import pychain as pc
from narwhals.typing import IntoLazyFrameT

from ._columns import Column


class Schema:
    _is_folder = True
    _schema: dict[str, Column]

    def __init_subclass__(cls) -> None:
        cls._set_schema()

    @classmethod
    def _set_schema(cls) -> type[Self]:
        cls._schema = {}
        for name, obj in cls.__dict__.items():
            if Column.__identity__(obj):
                obj.__from_schema__(name)
                cls._schema[name] = obj
            else:
                continue
        return cls

    @classmethod
    def columns(cls) -> pc.Dict[str, Column]:
        """
        Returns a dictionary of the Column instances in the folder.
        """
        return pc.Dict(cls._schema)

    @classmethod
    def cast(cls, df: IntoLazyFrameT) -> IntoLazyFrameT:
        return (
            nw.from_native(df)
            .lazy()
            .select([col.col.cast(col.dtype) for col in cls._schema.values()])
            .to_native()
        )
