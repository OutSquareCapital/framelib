from pathlib import Path

from ._core import Layout
from ._filehandlers import File
from ._tree import TreeBuilder

SOURCE = "__source__"


class Folder(Layout[File]):
    """A Folder represents a directory containing files.

    It's a `Schema` of `File` entries.
    """

    def __new__(cls) -> None:
        msg = "Folder cannot be instantiated directly."
        raise TypeError(msg)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if not hasattr(cls, SOURCE):
            cls.__source__ = Path()

        cls.__source__ = cls.__source__.joinpath(cls.__name__.lower())
        (
            cls.schema()
            .values()
            .iter()
            .for_each(lambda file: file.__set_source__(cls.source()))
        )

    @classmethod
    def source(cls) -> Path:
        """Get the source path of the folder.

        Returns:
            Path: the source path of the folder.
        """
        return cls.__source__

    @classmethod
    def show_tree(cls) -> str:
        """Show the folder structure as a tree.

        Returns:
            str: The folder structure.

        Example:
        ```python
        >>> import framelib as fl
        >>> class MyFolder(fl.Folder):
        ...     data = fl.CSV()
        ...     logs = fl.Json()
        >>> print(MyFolder.show_tree()) # doctest: +NORMALIZE_WHITESPACE
        myfolder
        ├── data.csv
        └── logs.json

        ```
        """
        return TreeBuilder.from_mro(cls.mro()).build()
