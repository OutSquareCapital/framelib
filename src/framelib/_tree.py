from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

import pychain as pc

if TYPE_CHECKING:
    from framelib._folder import Folder


class Tree(StrEnum):
    NODE = "├── "
    LAST_NODE = "└── "
    BRANCH = "│   "
    SPACE = "    "


class FolderStructure(NamedTuple):
    all_paths: pc.Iter[Path]
    dir_paths: set[Path]
    root: Path


def _folders_from_hierarchy(hierarchy: Iterable[type]) -> pc.Iter[type[Folder]]:
    from ._folder import Folder

    return pc.Iter(hierarchy).filter_subclass(Folder, keep_parent=False).apply(list)


def _source_from_schema(folder: type[Folder]) -> Iterable[Path]:
    return folder.schema().iter_values().map(lambda f: f.source).unwrap()


def _relative_to_root(folders: pc.Iter[type[Folder]], root: Path) -> list[Path]:
    return (
        folders.map(_source_from_schema)
        .explode()
        .filter_except(lambda p: p.relative_to(root), ValueError)
        .into(list)
    )


def _get_all_paths(hierarchy: Iterable[type]) -> FolderStructure:
    folders = _folders_from_hierarchy(hierarchy)
    root = folders.last().source()
    relatives = folders.pipe(_relative_to_root, root)

    dir_paths: set[Path] = set()
    for p in relatives:
        rel: Path = p.relative_to(root)
        parent: Path = rel.parent
        while str(parent) != ".":
            dir_paths.add(root.joinpath(parent))
            parent = parent.parent
    dir_paths.add(root)
    return FolderStructure(pc.Iter(set(relatives) | dir_paths), dir_paths, root)


def show_tree(hierarchy: Iterable[type]) -> str:
    structure = _get_all_paths(hierarchy)

    lines: list[str] = []

    def recurse(current: Path, prefix: str = "") -> None:
        children = structure.all_paths.filter(lambda p: p.parent == current).sort()
        children_len: int = children.length()
        for idx, child in children.enumerate().unwrap():
            is_last: bool = idx == children_len - 1
            lines.append(f"{prefix}{_connector(is_last)}{child.name}")
            if child in structure.dir_paths:
                recurse(child, prefix + _continuation(is_last))

    recurse(structure.root)
    return pc.Iter(lines).into(lambda x: f"{structure.root}\n" + "\n".join(x))


def _connector(is_last: bool) -> Tree:
    return Tree.LAST_NODE if is_last else Tree.NODE


def _continuation(is_last: bool) -> Tree:
    return Tree.SPACE if is_last else Tree.BRANCH
