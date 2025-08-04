from dataclasses import dataclass
from html import escape
from pathlib import Path

from ._types import CSS, Tree


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
    title: str
    css: str = CSS.STYLE

    def __repr__(self) -> str:
        header: str = f"{self.title}(directory='{self.root}')\n"
        tree_lines: list[str] = _build_tree(self.root)
        return header + "\n".join(tree_lines)

    def _repr_html_(self) -> str:
        header: str = (
            f"<strong>{self.title}</strong>(directory='<code>{self.root}</code>')"
        )
        html_tree: str = _build_html_tree(self.root)
        return f'<div class="tree">{self.css}{header}{html_tree}</div>'
