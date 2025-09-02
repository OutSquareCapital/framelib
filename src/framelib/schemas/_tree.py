from dataclasses import dataclass
from enum import StrEnum, auto
from html import escape
from pathlib import Path


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
            .tree li.file::after {
                background-image: url('data:image/svg+xml,...'); /* Icône fichier SVG (optionnel) */
            }
            .tree li.dir::after {
                background-image: url('data:image/svg+xml,...'); /* Icône dossier SVG (optionnel) */
            }
        </style>
        """


def _build_tree(directory: Path, prefix: str = "") -> list[str]:
    lines: list[str] = []
    items: list[Path] = _get_items(directory)
    for i, item in enumerate(items):
        is_last: bool = i == (len(items) - 1)
        connector = Tree.LAST_NODE if is_last else Tree.NODE
        lines.append(f"{prefix}{connector}{item.name}")
        if item.is_dir():
            new_prefix: str = prefix + (Tree.SPACE if is_last else Tree.BRANCH)
            lines.extend(_build_tree(item, new_prefix))
    return lines


def _build_html_tree(directory: Path) -> str:
    html = "<ul>"
    items: list[Path] = _get_items(directory)
    for item in items:
        css_class = CSS.DIR if item.is_dir() else CSS.FILE
        html += f'<li class="{css_class}">{escape(item.name)}'
        if item.is_dir():
            html += _build_html_tree(item)
        html += "</li>"
    html += "</ul>"
    return html


def _get_items(directory: Path) -> list[Path]:
    return sorted(
        list(directory.iterdir()), key=lambda p: (p.is_file(), p.name.lower())
    )


@dataclass(slots=True, repr=False)
class TreeDisplay:
    root: Path

    def __repr__(self) -> str:
        header: str = f"{self.root}\n"
        tree_lines: list[str] = _build_tree(self.root)
        return header + "\n".join(tree_lines)

    def _repr_html_(self) -> str:
        return f'<div class="tree">{CSS.STYLE}{self._header}{_build_html_tree(self.root)}</div>'

    @property
    def _header(self) -> str:
        return f"'<code>{self.root}</code>'"
