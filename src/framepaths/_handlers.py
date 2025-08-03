from pathlib import Path as _Path
from typing import Literal

from dataframely import Schema as _Schema

from ._tree import TreeDisplay

Formatting = Literal["upper", "lower", "title"]


class Handler(_Schema):
    """
    Base handler for file-based schemas, providing path and tree display utilities.
    """

    __directory__: str | _Path = ""
    __ext__: str = ""

    @classmethod
    def path(cls, make_dir: bool = False, format: Formatting | None = None) -> _Path:
        """
        Returns the full path to the file for this schema, optionally creating parent directories.
        """
        name = cls.__name__
        match format:
            case "upper":
                name = name.upper()
            case "lower":
                name = name.lower()
            case "title":
                name = name.title()
            case _:
                pass
        path = _Path(cls.__directory__).joinpath(f"{name}{cls.__ext__}")
        if make_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def show_tree(cls) -> TreeDisplay:
        """
        Returns a TreeDisplay object for the directory containing this schema's file.
        """
        root_dir = _Path(cls.path().parent)
        return TreeDisplay(root=root_dir, title=cls.__name__)


class CSV(Handler):
    """
    Handler for CSV file schemas.
    """

    __ext__ = ".csv"


class Parquet(Handler):
    """
    Handler for Parquet file schemas.
    """

    __ext__ = ".parquet"


class NDJSON(Handler):
    """
    Handler for NDJSON file schemas.
    """

    __ext__ = ".ndjson"
