from typing import Any

import dataframely as dy
import polars as pl
import pyarrow as pa
from dataframely.random import Generator


class Categorical(dy.Column):
    @property
    def dtype(self) -> pl.DataType:
        return pl.Categorical  # type: ignore

    def sqlalchemy_dtype(self, dialect: Any): ...
    @property
    def pyarrow_dtype(self) -> pa.DataType: ...
    def _sample_unchecked(self, generator: Generator, n: int) -> pl.Series: ...
