from typing import Any

import dataframely as dy
import polars as pl
from dataframely.random import Generator


class Categorical(dy.Column):
    @property
    def dtype(self) -> pl.DataType:
        return pl.Categorical()

    def sqlalchemy_dtype(self, dialect: Any): ...
    @property
    def pyarrow_dtype(self): ...
    def _sample_unchecked(self, generator: Generator, n: int) -> pl.Series: ...
