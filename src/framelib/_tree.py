from dataclasses import dataclass
from enum import StrEnum, auto
from html import escape
from pathlib import Path

import pychain as pc


class Tree(StrEnum):
    NODE = "├── "
    LAST_NODE = "└── "
    BRANCH = "│   "
    SPACE = "    "


class CSS(StrEnum):
    DIR = auto()
    FILE = auto()
    STYLE = """
        <style>
            .tree ul { list-style-type: none; padding-left: 20px; }
            .tree li { position: relative; padding-left: 25px; line-height: 1.8; }
            .tree li::before, .tree li::after {
                content: ''; position: absolute; left: 0;
            }
            .tree li::before {
                border-left: 1px solid #999; height: 100%; top: 0; width: 0;
            }
            .tree li:last-child::before { height: 20px; }
            .tree li::after {
                border-top: 1px solid #999; height: 0; top: 20px; width: 18px;
            }
        </style>
        """


def _build_tree(directory: Path, prefix: str = "") -> list[str]:
    lines: list[str] = []
    items: pc.Iter[Path] = _paths(directory)
    items_length: int = items.length()

    for i, item in items.enumerate().unwrap():
        is_last: bool = i == (items_length - 1)
        lines.append(f"{prefix}{_connector(is_last)}{item.name}")
        if item.is_dir():
            lines.extend(_build_tree(item, prefix + _continuation(is_last)))
    return lines


def _build_html_tree(directory: Path) -> str:
    html = "<ul>"
    _paths(directory).map(lambda path: _map_paths(html, path))
    html += "</ul>"
    return html


def _map_paths(html: str, path: Path) -> None:
    html += f'<li class="{_class(path)}">{escape(path.name)}'
    if path.is_dir():
        html += _build_html_tree(path)
    html += "</li>"


def _paths(directory: Path) -> pc.Iter[Path]:
    return pc.Iter(directory.iterdir()).sort(
        key=lambda p: (p.is_file(), p.name.lower())
    )


def _class(item: Path) -> CSS:
    return CSS.DIR if item.is_dir() else CSS.FILE


def _connector(is_last: bool) -> Tree:
    return Tree.LAST_NODE if is_last else Tree.NODE


def _continuation(is_last: bool) -> Tree:
    return Tree.SPACE if is_last else Tree.BRANCH


@dataclass(slots=True, repr=False)
class TreeDisplay:
    root: Path

    def __repr__(self) -> str:
        return f"{self.root}\n" + "\n".join(_build_tree(self.root))

    def _repr_html_(self) -> str:
        return f'<div class="tree">{CSS.STYLE}{self._html_header}{_build_html_tree(self.root)}</div>'

    @property
    def _html_header(self) -> str:
        return f"'<code>{self.root}</code>'"
