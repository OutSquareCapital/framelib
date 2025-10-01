from __future__ import annotations

import io
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Final

import dataframely as dy
import pychain as pc
import requests
from yarl import URL

from .._core import BaseLayout, Entry, EntryType


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


@dataclass(slots=True)
class JSON[T](Request[T]):
    def get(self) -> pc.Wrapper[T]:
        """Query and parse the response as JSON."""
        return pc.Wrapper(self._get().json())


@dataclass(slots=True)
class CSV[T: dy.Schema](Request[T]):
    def get(self) -> pc.Wrapper[io.BytesIO]:
        """Query and parse the response as a BytesIO."""
        return pc.Wrapper(io.BytesIO(self._get().content))


class Server(BaseLayout[Request[Any]]):
    _is_entry_type = EntryType.REQUEST
