from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, Self

import dataframely as dy
import duckdb
import narwhals as nw
import polars as pl
import pychain as pc
import requests
from narwhals.typing import IntoFrame, IntoLazyFrame
from yarl import URL

from .._core import Entry
from .._schema import Schema


class Request[T](Entry[T, URL]):
    _is_request: Final[bool] = True

    def _get(self) -> requests.Response:
        """
        Request the URL and return a QueryResult.

        Raise a ValueError if the request fails.
        """
        resp: requests.Response = requests.get(url=self.source.human_repr())
        if resp.status_code != 200:
            raise ValueError(self._error(response=resp))
        return resp

    def _error(self, response: requests.Response) -> str:
        return f"Failed to fetch data from {self.source}: {response.status_code} {response.reason}"

    @abstractmethod
    def get(self) -> pc.Wrapper[Any]:
        """Query and parse the response."""
        raise NotImplementedError


class File[T: dy.Schema](Entry[T, Path]):
    _is_file: Final[bool] = True
    _with_suffix: bool = True

    def _build_source(self, source: Path | str) -> None:
        self.source = Path(source, self._name)
        if self.__class__._with_suffix:
            self.source = self.source.with_suffix(f".{self.__class__.__name__.lower()}")

    @property
    @abstractmethod
    def read(self) -> Callable[..., pl.DataFrame]:
        raise NotImplementedError

    @property
    def scan(self) -> Callable[..., pl.LazyFrame]:
        raise NotImplementedError

    @property
    @abstractmethod
    def write(self) -> Any:
        raise NotImplementedError

    def read_cast(self) -> pl.DataFrame:
        """
        Read the file and cast it to the defined schema.
        """
        return self.read().pipe(self.model.cast)

    def scan_cast(self) -> pl.LazyFrame:
        """
        Scan the file and cast it to the defined schema.
        """
        return self.scan().pipe(self.model.cast)

    def write_cast(
        self, df: pl.LazyFrame | pl.DataFrame, *args: Any, **kwargs: Any
    ) -> None:
        """
        Cast the dataframe to the defined schema and write it to the file.
        """
        self.model.cast(df.lazy().collect()).pipe(self.write, *args, **kwargs)


class Table[T: Schema](Entry[T, duckdb.DuckDBPyConnection]):
    _is_table: Final[bool] = True

    def with_connexion(self, con: duckdb.DuckDBPyConnection) -> Self:
        self._con = con
        return self

    def read(self) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
        return nw.from_native(self._con.table(self._name))

    def write(self, df: IntoFrame | IntoLazyFrame) -> None:
        _ = nw.from_native(df).lazy().collect().to_native()
        self._con.execute(f"CREATE OR REPLACE TABLE {self._name} AS SELECT * FROM _")
