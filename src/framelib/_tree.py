from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

import pychain as pc

if TYPE_CHECKING:
    from framelib._folder import Folder


class Tree(StrEnum):
    NODE = "├── "
    LAST_NODE = "└── "
    BRANCH = "│   "
    SPACE = "    "


def show_tree(hierarchy: Iterable[type]) -> str:
    from ._folder import Folder

    folders: pc.Iter[type[Folder]] = (
        pc.Iter(hierarchy).filter_subclass(Folder, keep_parent=False).apply(list)
    )

    root = folders.last().source()

    scoped: list[Path] = (
        folders.map(lambda c: c.schema().iter_values().map(lambda f: f.source).unwrap())
        .explode()
        .filter_except(lambda p: p.relative_to(root), ValueError)
        .into(list)
    )
    dir_paths: set[Path] = set()
    for p in scoped:
        rel: Path = p.relative_to(root)
        parent: Path = rel.parent
        while str(parent) != ".":
            dir_paths.add(root.joinpath(parent))
            parent = parent.parent
    dir_paths.add(root)
    all_paths: pc.Iter[Path] = pc.Iter(set(scoped) | dir_paths)

    lines: list[str] = []

    def recurse(current: Path, prefix: str = "") -> None:
        children: pc.Iter[Path] = all_paths.filter(lambda p: p.parent == current).sort()
        children_len: int = children.length()
        for idx, child in children.enumerate().unwrap():
            is_last: bool = idx == children_len - 1
            lines.append(f"{prefix}{_connector(is_last)}{child.name}")
            if child in dir_paths:
                recurse(child, prefix + _continuation(is_last))

    recurse(root)
    return pc.Iter(lines).into(lambda x: f"{root}\n" + "\n".join(x))


def _connector(is_last: bool) -> Tree:
    return Tree.LAST_NODE if is_last else Tree.NODE


def _continuation(is_last: bool) -> Tree:
    return Tree.SPACE if is_last else Tree.BRANCH
