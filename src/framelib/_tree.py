from enum import StrEnum
from pathlib import Path

import pychain as pc


def show_tree(path: Path) -> str:
    """
    Returns a string representation of the directory tree starting from the given path.
    """
    return f"{path}\n" + "\n".join(_build_tree(path))


class Tree(StrEnum):
    NODE = "├── "
    LAST_NODE = "└── "
    BRANCH = "│   "
    SPACE = "    "


def _build_tree(directory: Path, prefix: str = "") -> list[str]:
    lines: list[str] = []
    items: pc.Iter[Path] = pc.Iter(directory.iterdir()).sort(
        key=lambda p: (p.is_file(), p.name.lower())
    )

    items_length: int = items.length()
    for i, item in items.enumerate().unwrap():
        is_last: bool = i == (items_length - 1)
        lines.append(f"{prefix}{_connector(is_last)}{item.name}")
        if item.is_dir():
            lines.extend(_build_tree(item, prefix + _continuation(is_last)))
    return lines


def _connector(is_last: bool) -> Tree:
    return Tree.LAST_NODE if is_last else Tree.NODE


def _continuation(is_last: bool) -> Tree:
    return Tree.SPACE if is_last else Tree.BRANCH
