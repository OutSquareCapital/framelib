from __future__ import annotations

import io
from dataclasses import dataclass

import dataframely as dy
import pychain as pc

from ._entries import Request


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
