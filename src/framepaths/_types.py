from enum import StrEnum, auto
from typing import Literal

Formatting = Literal["upper", "lower", "title"]


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
