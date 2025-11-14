from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum
from pathlib import Path
from typing import NamedTuple

import pyochain as pc


class Leaf(StrEnum):
    NEW = "├── "
    LAST = "└── "


def _leaf_line(*, is_last: bool) -> Leaf:
    return Leaf.LAST if is_last else Leaf.NEW


class Tree(StrEnum):
    BRANCH = "│   "
    SPACE = "    "


def _tree_line(*, is_last: bool) -> Tree:
    return Tree.SPACE if is_last else Tree.BRANCH


class FolderStructure(NamedTuple):
    all_paths: pc.Seq[Path]
    dir_paths: set[Path]
    root: Path


def show_tree(hierarchy: Sequence[type]) -> str:
    from ._folder import Folder

    folders = (
        pc.Seq(hierarchy).iter().filter_subclass(Folder, keep_parent=False).collect()
    )

    root: Path = folders.last().source()
    relatives: pc.Seq[Path] = (
        folders.iter()
        .map(
            lambda folder: (
                folder.schema().iter_values().map(lambda f: f.source).inner()
            ),
        )
        .flatten()
        .filter_except(lambda p: p.relative_to(root), ValueError)
        .collect()
    )
    dir_paths: set[Path] = {root}

    def _add_to_root(p: Path) -> None:
        parent: Path = p.relative_to(root).parent
        while str(parent) != ".":
            dir_paths.add(root / parent)
            parent = parent.parent

    relatives.iter().for_each(_add_to_root)
    structure = relatives.union(dir_paths).pipe(FolderStructure, dir_paths, root)
    lines: list[str] = []

    def recurse(current: Path, prefix: str = "") -> None:
        children = (
            structure.all_paths.iter()
            .filter(lambda path: path.parent == current)
            .sort()
        )

        children_len: int = children.count()

        def _visit(entry: tuple[int, Path]) -> None:
            idx, child = entry
            is_last: bool = idx == children_len - 1
            lines.append(f"{prefix}{_leaf_line(is_last=is_last)}{child.name}")
            if child in structure.dir_paths:
                recurse(child, prefix + _tree_line(is_last=is_last))

        (children.iter().enumerate().for_each(_visit))

    recurse(structure.root)
    return pc.Seq(lines).iter().into(lambda xs: f"{structure.root}\n" + "\n".join(xs))
