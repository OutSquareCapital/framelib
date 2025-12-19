from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

import pyochain as pc

if TYPE_CHECKING:
    from ._folder import File, Folder


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


@dataclass(slots=True)
class FolderStructure:
    all_paths: pc.Seq[Path]
    dir_paths: set[Path]

    def childrens(self, current: Path) -> pc.Seq[Path]:
        return self.all_paths.iter().filter(lambda path: path.parent == current).sort()


def _folders_to_structure(folders: pc.Seq[type[Folder]], root: Path) -> FolderStructure:
    dir_paths: set[Path] = {root}

    def _add_to_tree(folder: File) -> pc.Option[Path]:
        try:
            parent: Path = folder.source.relative_to(root).parent
            while str(parent) != ".":
                dir_paths.add(root.joinpath(parent))
                parent = parent.parent
            return pc.Some(folder.source)
        except ValueError:
            return pc.NONE

    return (
        folders.iter()
        .map(lambda f: f.schema().iter_values().filter_map(_add_to_tree).inner())
        .flatten()
        .union(dir_paths)
        .pipe(FolderStructure, dir_paths)
    )


def show_tree(hierarchy: Sequence[type]) -> str:
    from ._folder import Folder

    folders = (
        pc.Seq(hierarchy).iter().filter_subclass(Folder, keep_parent=False).collect()
    )

    root: Path = folders.last().source()
    structure: FolderStructure = _folders_to_structure(folders, root)
    lines: list[str] = []

    def recurse(current: Path, prefix: str = "") -> None:
        childrens = structure.childrens(current)
        children_len: int = childrens.count()

        def _visit(idx: int, child: Path) -> None:
            is_last: bool = idx == children_len - 1
            lines.append(f"{prefix}{_leaf_line(is_last=is_last)}{child.name}")
            if child in structure.dir_paths:
                recurse(child, prefix + _tree_line(is_last=is_last))

        childrens.iter().enumerate().for_each(lambda entry: _visit(*entry))

    recurse(root)
    return pc.Seq(lines).iter().into(lambda xs: f"{root}\n" + "\n".join(xs))
