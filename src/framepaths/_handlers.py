from pathlib import Path as _Path

from dataframely import Schema as _Schema

from ._tree import TreeDisplay


class Handler(_Schema):
    __directory__: str | _Path = ""
    __ext__: str = ""

    @classmethod
    def path(cls, make_dir: bool = False) -> _Path:
        path = _Path(cls.__directory__).joinpath(f"{cls.__name__.lower()}{cls.__ext__}")
        if make_dir:
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def show_tree(cls) -> TreeDisplay:
        root_dir = _Path(cls.path().parent)
        return TreeDisplay(root=root_dir, title=cls.__name__)


class CSV(Handler):
    __ext__ = ".csv"


class Parquet(Handler):
    __ext__ = ".parquet"


class NDJSON(Handler):
    __ext__ = ".ndjson"
