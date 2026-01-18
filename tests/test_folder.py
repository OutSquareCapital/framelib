"""Tests for Folder tree rendering and Schema casting / multi-column PK behavior."""

from pathlib import Path

import framelib as fl


def test_folder_show_tree_and_file_sources(tmp_path: Path) -> None:
    """Folder show_tree lists files with correct suffixes."""

    class S(fl.Schema):
        id = fl.Int64()

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        data = fl.Parquet(model=S)
        logs = fl.Json(model=S)

    # ensure folder exists
    Project.source().mkdir(parents=True, exist_ok=True)

    tree = Project.show_tree()
    # root path should be included and file names should appear with correct suffixes
    assert str(Project.source()) in tree
    assert "data.parquet" in tree
    assert "logs.json" in tree
