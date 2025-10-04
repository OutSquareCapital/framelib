from collections.abc import Iterable
from enum import StrEnum
from pathlib import Path

import pychain as pc


def show_tree(path: Path, expected: Iterable[Path] | None = None) -> str:
    if expected:
        return f"{path}\n" + "\n".join(_build_tree(path, expected))
    return f"{path}"


class Tree(StrEnum):
    NODE = "├── "
    LAST_NODE = "└── "
    BRANCH = "│   "
    SPACE = "    "


def _build_tree(root: Path, expected: Iterable[Path]) -> list[str]:
    scoped: list[Path] = []
    for p in expected:
        try:
            p.relative_to(root)
        except ValueError:
            continue
        scoped.append(p)
    dir_paths: set[Path] = set()
    for p in scoped:
        rel: Path = p.relative_to(root)
        parent: Path = rel.parent
        while str(parent) != ".":
            dir_paths.add(root.joinpath(parent))
            parent = parent.parent
    dir_paths.add(root)
    all_paths: set[Path] = set(scoped) | dir_paths

    lines: list[str] = []

    def recurse(current: Path, prefix: str = "") -> None:
        children: pc.Iter[Path] = (
            pc.Iter(all_paths)
            .filter(lambda p: p.parent == current)
            .sort(key=lambda p: (p not in dir_paths, p.name.lower()))
        )
        children_len: int = children.length()
        for idx, child in children.enumerate().unwrap():
            is_last: bool = idx == children_len - 1
            lines.append(f"{prefix}{_connector(is_last)}{child.name}")
            if child in dir_paths:
                recurse(child, prefix + _continuation(is_last))

    recurse(root)
    return lines


def _connector(is_last: bool) -> Tree:
    return Tree.LAST_NODE if is_last else Tree.NODE


def _continuation(is_last: bool) -> Tree:
    return Tree.SPACE if is_last else Tree.BRANCH
