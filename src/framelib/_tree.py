from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Self, TypeIs

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
class TreeBuilder:
    folders: pc.Seq[type[Folder]]
    root: Path
    structure: Structure = field(init=False)

    @classmethod
    def from_mro(cls, mro: Sequence[type]) -> Self:
        from ._folder import Folder

        def _is_subclass[T](cls: type, parent: type[T]) -> TypeIs[type[T]]:
            return issubclass(cls, parent) and cls is not parent

        return (
            pc.Iter(mro)
            .filter(lambda cls: _is_subclass(cls, Folder))
            .collect()
            .into(lambda folders: cls(folders, folders.last().source()))
        )

    def build(self) -> str:
        def _add_to_tree(folder: File) -> pc.Iter[Path]:
            try:
                return pc.Iter.successors(
                    pc.Some(folder.source.relative_to(self.root).parent),
                    lambda p: pc.Some(p.parent) if p.as_posix() != "." else pc.NONE,
                ).map(lambda p: self.root.joinpath(p))
            except ValueError:
                return pc.Iter[Path].empty()

        return (
            self.folders.iter()
            .flat_map(lambda f: f.schema().values_iter())
            .flat_map(_add_to_tree)
            .insert(self.root)
            .collect(pc.Set)
            .into(Structure.from_folders, self.folders)
            .recurse(self.root)
            .collect()
            .then_some()
            .map(lambda lines: f"{self.root}\n{lines.join('\n')}")
            .unwrap_or(f"{self.root}\n")
        )


@dataclass(slots=True)
class Structure:
    all_paths: pc.Set[Path]
    dir_paths: pc.Set[Path]

    @classmethod
    def from_folders(
        cls, dir_paths: pc.Set[Path], all_folders: pc.Seq[type[Folder]]
    ) -> Self:
        return (
            all_folders.iter()
            .flat_map(lambda f: f.schema().values_iter().map(lambda file: file.source))
            .collect(pc.Set)
            .union(dir_paths)
            .into(cls, dir_paths)
        )

    def _childrens(self, current: Path) -> pc.Seq[Path]:
        return self.all_paths.iter().filter(lambda path: path.parent == current).sort()

    def recurse(self, current: Path, prefix: str = "") -> pc.Iter[str]:
        childrens = self._childrens(current)
        children_len: int = childrens.length()

        def _entries(entry: tuple[int, Path]) -> pc.Iter[str]:
            idx, node = entry
            match node in self.dir_paths:
                case True:
                    return pc.Iter.once(
                        f"{prefix}{Leaf.line(is_last=idx == children_len - 1)}{node.name}"
                    )
                case False:
                    return pc.Iter.once(
                        f"{prefix}{Leaf.line(is_last=idx == children_len - 1)}{node.name}"
                    ).chain(
                        self.recurse(
                            node,
                            f"{prefix}{Tree.line(is_last=idx == children_len - 1)}",
                        )
                    )

        return childrens.iter().enumerate().flat_map(_entries)
