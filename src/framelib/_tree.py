from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, TypeIs

import pyochain as pc

if TYPE_CHECKING:
    from ._folder import File, Folder


class Leaf(StrEnum):
    NEW = "├── "
    LAST = "└── "

    @classmethod
    def line(cls, *, is_last: bool) -> Leaf:
        return cls.LAST if is_last else cls.NEW


class Tree(StrEnum):
    BRANCH = "│   "
    SPACE = "    "

    @classmethod
    def line(cls, *, is_last: bool) -> Tree:
        return cls.SPACE if is_last else cls.BRANCH


@dataclass(slots=True)
class FolderStructure:
    all_paths: pc.Set[Path]
    dir_paths: pc.Set[Path]

    def childrens(self, current: Path) -> pc.Seq[Path]:
        return self.all_paths.iter().filter(lambda path: path.parent == current).sort()


def _is_subclass[T](cls: type, parent: type[T]) -> TypeIs[type[T]]:
    return issubclass(cls, parent) and cls is not parent


def show_tree(hierarchy: Sequence[type]) -> str:
    from ._folder import Folder

    folders = pc.Iter(hierarchy).filter(lambda cls: _is_subclass(cls, Folder)).collect()

    root: Path = folders.last().source()
    structure: FolderStructure = folders.into(_folders_to_structure, root)
    lines = pc.Vec[str].new()

    def recurse(current: Path, prefix: str = "") -> None:
        childrens = structure.childrens(current)
        children_len: int = childrens.length()

        def _visit(entry: pc.Enumerated[Path]) -> None:
            is_last: bool = entry.idx == children_len - 1
            lines.append(f"{prefix}{Leaf.line(is_last=is_last)}{entry.value.name}")
            if entry.value in structure.dir_paths:
                recurse(entry.value, prefix + Tree.line(is_last=is_last))

        return childrens.iter().enumerate().for_each(_visit)

    recurse(root)
    return lines.into(lambda xs: f"{root}\n" + xs.join("\n"))


def _folders_to_structure(folders: pc.Seq[type[Folder]], root: Path) -> FolderStructure:
    dir_paths = pc.SetMut({root})

    def _add_to_tree(folder: File) -> pc.Option[Path]:
        try:
            parent: Path = folder.source.relative_to(root).parent
            while parent.as_posix() != ".":
                dir_paths.add(root.joinpath(parent))
                parent = parent.parent
            return pc.Some(folder.source)
        except ValueError:
            return pc.NONE

    return (
        folders.iter()
        .flat_map(lambda f: f.schema().values_iter().filter_map(_add_to_tree))
        .collect(pc.Set)
        .union(dir_paths)
        .into(FolderStructure, dir_paths)
    )
