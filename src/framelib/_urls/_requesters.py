from __future__ import annotations

import io
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import dataframely as dy
import pychain as pc
import requests
from yarl import URL

CONNEXION = requests.Session()


@dataclass(slots=True)
class BaseRequester[T](ABC):
    """Expose methods to parse a requests.Response from an URL"""

    url: URL
    model: type[T]

    def _get(self) -> requests.Response:
        """
        Request the URL and return a QueryResult.

        Raise a ValueError if the request fails.
        """
        resp: requests.Response = CONNEXION.get(url=self.url.human_repr())
        if resp.status_code != 200:
            raise ValueError(self._error(response=resp))
        return resp

    def _error(self, response: requests.Response) -> str:
        return f"Failed to fetch data from {self.url}: {response.status_code} {response.reason}"

    @abstractmethod
    def get(self) -> pc.Wrapper[Any]:
        """Query and parse the response."""
        raise NotImplementedError


@dataclass(slots=True)
class JSON[T](BaseRequester[T]):
    def get(self) -> pc.Wrapper[T]:
        """Query and parse the response as JSON."""
        return pc.Wrapper(self._get().json())


@dataclass(slots=True)
class CSV[T: dy.Schema](BaseRequester[T]):
    def get(self) -> pc.Wrapper[io.BytesIO]:
        """Query and parse the response as a BytesIO."""
        return pc.Wrapper(io.BytesIO(self._get().content))
