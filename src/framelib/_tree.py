from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Self, TypeIs

from pyochain import Iter, Option, Seq, Set, Some, Vec, then_if_true

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

    from pyochain.abc import PyoIterator

    from ._filehandlers import File
    from ._folder import Folder


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
class TreeBuilder:
    folders: Seq[type[Folder]]
    root: Path
    structure: Structure = field(init=False)

    @classmethod
    def from_mro(cls, mro: Sequence[type]) -> Self:
        from ._folder import Folder

        def _is_subclass[T](cls: type, parent: type[T]) -> TypeIs[type[T]]:
            return issubclass(cls, parent) and cls is not parent

        return (
            Iter(mro)
            .filter(lambda cls: _is_subclass(cls, Folder))
            .collect(Seq)
            .into(lambda folders: cls(folders, folders.last().source()))
        )

    def build(self) -> str:
        def _add_to_tree(folder: File) -> PyoIterator[Path]:
            def _is_addable(path: Path) -> Option[Path]:
                return then_if_true(
                    path.parent, predicate=lambda p: p.as_posix() != "."
                )

            try:
                return (
                    Some(folder.source.relative_to(self.root).parent)
                    .into(Iter.successors, succ=_is_addable)
                    .map(self.root.joinpath)
                )
            except ValueError:
                return Iter(())

        return (
            self.folders
            .iter()
            .flat_map(lambda f: f.entries().values())
            .flat_map(_add_to_tree)
            .insert(self.root)
            .collect(Set)
            .into(Structure.from_folders, self.folders)
            .recurse(self.root)
            .collect(Seq)
            .then(lambda lines: f"{self.root}\n{lines.iter().join('\n')}")
            .unwrap_or(f"{self.root}\n")
        )


@dataclass(slots=True)
class Structure:
    all_paths: Set[Path]
    dir_paths: Set[Path]

    @classmethod
    def from_folders(cls, dir_paths: Set[Path], all_folders: Seq[type[Folder]]) -> Self:
        return (
            all_folders
            .iter()
            .flat_map(
                lambda f: f.entries().values().iter().map(lambda file: file.source)
            )
            .collect(Set)
            .union(dir_paths)
            .into(cls, dir_paths)
        )

    def _childrens(self, current: Path) -> Vec[Path]:
        return self.all_paths.iter().filter(lambda path: path.parent == current).sort()

    def recurse(self, current: Path, prefix: str = "") -> PyoIterator[str]:
        childrens = self._childrens(current)
        children_len: int = childrens.len()

        def _entries(idx: int, node: Path) -> PyoIterator[str]:
            is_last = idx == children_len - 1
            line = f"{prefix}{Leaf.line(is_last=is_last)}{node.name}"
            match node in self.dir_paths:
                case True:  # Directory: print and recurse into it
                    return Iter.once(line).chain(
                        self.recurse(
                            node,
                            f"{prefix}{Tree.line(is_last=is_last)}",
                        )
                    )
                case False:  # File: just print
                    return Iter.once(line)

        return childrens.iter().enumerate().map_star(_entries).flatten()
