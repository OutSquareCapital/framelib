"""Tests for Folder tree rendering, source attribute construction & distribution, inheritance."""

from pathlib import Path

import pyochain as pc

import framelib as fl


def _simple_schema() -> type[fl.Schema]:
    """Factory for a simple test schema to reduce repetition."""

    class S(fl.Schema):
        id = fl.Int64()

    return S


def _assert_file_in_folder(
    file_path: Path, folder_path: Path, filename: str, ext: str
) -> None:
    """Helper to assert complete file path structure."""
    file_str = str(file_path)
    folder_str = str(folder_path)
    assert folder_str in file_str, f"Folder {folder_str} not in file {file_str}"
    assert file_str.endswith(f"{filename}.{ext}"), (
        f"File doesn't end with {filename}.{ext}: {file_str}"
    )
    assert file_path.name == f"{filename}.{ext}", (
        f"File name is {file_path.name}, expected {filename}.{ext}"
    )


def _assert_path_hierarchy(parent: Path, child: Path, child_name: str) -> None:
    """Helper to assert parent-child path relationship."""
    parent_str = str(parent)
    child_str = str(child)
    assert parent_str in child_str, f"Parent {parent_str} not in child {child_str}"
    assert child_str.lower().endswith(child_name.lower()), (
        f"Child doesn't end with {child_name}: {child_str}"
    )
    assert parent_str != child_str, "Parent and child paths should be different"


# ============================================================================
# Basic Tests: Source Path Creation
# ============================================================================


def test_folder_source_path_basic_creation(tmp_path: Path) -> None:
    """Folder automatically appends lowercase class name to source path."""

    class MyData(fl.Folder):
        __source__ = Path(tmp_path)

    source = MyData.source()
    assert "mydata" in str(source).lower()
    assert Path(tmp_path) in source.parents
    assert source.name == "mydata"


def test_folder_default_source_without_explicit_source() -> None:
    """Folder without __source__ uses Path() and appends class name."""

    class NoSourceFolder(fl.Folder):
        pass

    source = NoSourceFolder.source()
    assert "nosourcefolder" in str(source).lower()


def test_folder_source_with_nested_explicit_paths(tmp_path: Path) -> None:
    """Source path is properly nested when using joinpath."""
    base = tmp_path.joinpath("data", "raw")

    class CustomFolder(fl.Folder):
        __source__ = base

    source = CustomFolder.source()
    assert base in source.parents or source.parent == base
    assert "customfolder" in str(source).lower()
    # Verify full path includes all parts
    source_parts = source.parts
    assert "data" in source_parts
    assert "raw" in source_parts
    assert "customfolder" in str(source).lower()


# ============================================================================
# File Discovery Tests: Tree Rendering & File Sources
# ============================================================================


def test_folder_show_tree_and_file_sources(tmp_path: Path) -> None:
    """Folder show_tree lists files with correct suffixes and includes root path."""
    schema = _simple_schema()

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        data = fl.Parquet(model=schema)
        logs = fl.Json(model=schema)

    tree = Project.show_tree()
    project_path = str(Project.source())
    assert project_path in tree, f"Root path '{project_path}' not in tree:\n{tree}"
    assert "data.parquet" in tree, f"data.parquet not in tree:\n{tree}"
    assert "logs.json" in tree, f"logs.json not in tree:\n{tree}"


def test_folder_tree_shows_all_file_extensions(tmp_path: Path) -> None:
    """Tree output includes all files with correct extensions."""
    schema = _simple_schema()

    class FileFolder(fl.Folder):
        __source__ = Path(tmp_path)
        csv_file = fl.CSV(model=schema)
        json_file = fl.Json(model=schema)
        parquet_file = fl.Parquet(model=schema)

    tree = FileFolder.show_tree()
    assert "csv_file.csv" in tree
    assert "json_file.json" in tree
    assert "parquet_file.parquet" in tree
    # Verify extensions don't mix
    assert ".csv" not in tree.replace("csv_file.csv", "")
    assert ".json" not in tree.replace("json_file.json", "")


def test_folder_tree_output_structure(tmp_path: Path) -> None:
    """Tree output uses proper tree syntax characters."""
    schema = _simple_schema()

    class TreeFolder(fl.Folder):
        __source__ = Path(tmp_path)
        file1 = fl.CSV(model=schema)
        file2 = fl.Json(model=schema)

    tree = TreeFolder.show_tree()
    # Should contain tree branch characters
    assert "├──" in tree or "└──" in tree or "─" in tree, f"No tree syntax in:\n{tree}"


# ============================================================================
# File Source Path Tests: Complete Path Verification
# ============================================================================


def test_folder_all_files_have_correct_source(tmp_path: Path) -> None:
    """All files in Folder have source set to folder source + filename with correct extensions."""
    schema = _simple_schema()

    class FullFolder(fl.Folder):
        __source__ = Path(tmp_path)
        csv_file = fl.CSV(model=schema)
        json_file = fl.Json(model=schema)
        parquet_file = fl.Parquet(model=schema)

    file_schema = FullFolder.schema()
    folder_path = FullFolder.source()

    pc.Iter(
        [
            ("csv_file", "csv"),
            ("json_file", "json"),
            ("parquet_file", "parquet"),
        ]
    ).map_star(
        lambda name, ext: (name, ext, file_schema.get_item(name).unwrap())
    ).for_each_star(
        lambda name, ext, file_obj: _assert_file_in_folder(
            file_obj.source, folder_path, name, ext
        )
    )

    # All sources must be different
    assert (
        pc.Iter(
            [
                ("csv_file", "csv"),
                ("json_file", "json"),
                ("parquet_file", "parquet"),
            ]
        )
        .map_star(lambda name, _: str(file_schema.get_item(name).unwrap().source))
        .collect(pc.Set)
        .length()
        == 3
    )


def test_folder_file_source_is_direct_child_path(tmp_path: Path) -> None:
    """File source path is a direct child of folder, not nested deeper."""
    schema = _simple_schema()

    class DirectChildFolder(fl.Folder):
        __source__ = Path(tmp_path)
        data = fl.CSV(model=schema)

    file_obj = DirectChildFolder.schema().get_item("data").unwrap()
    folder_path = DirectChildFolder.source()

    # File should be directly under folder
    assert file_obj.source.parent == folder_path, (
        f"File parent {file_obj.source.parent} != folder {folder_path}"
    )


def test_folder_nested_file_sources(tmp_path: Path) -> None:
    """Files in folder have source paths including complete folder hierarchy."""
    schema = _simple_schema()

    class DataFolder(fl.Folder):
        __source__ = Path(tmp_path)
        output = fl.CSV(model=schema)

    _assert_file_in_folder(
        DataFolder.schema().get_item("output").unwrap().source,
        DataFolder.source(),
        "output",
        "csv",
    )


# ============================================================================
# Inheritance Tests: Path Hierarchy & Structure Preservation
# ============================================================================


def test_folder_inheritance_basic(tmp_path: Path) -> None:
    """Inherited Folder creates proper path hierarchy and preserves parent files."""
    schema = _simple_schema()

    class BaseFolder(fl.Folder):
        __source__ = Path(tmp_path)
        base_data = fl.CSV(model=schema)

    class DerivedFolder(BaseFolder):
        pass

    tree = DerivedFolder.show_tree()
    assert "base_data.csv" in tree, f"base_data.csv not in derived tree:\n{tree}"
    _assert_path_hierarchy(BaseFolder.source(), DerivedFolder.source(), "derivedfolder")


def test_folder_inheritance_with_new_files(tmp_path: Path) -> None:
    """Inherited Folder with new files uses nested folder path, parent files unchanged."""
    schema = _simple_schema()

    class ParentFolder(fl.Folder):
        __source__ = Path(tmp_path)
        parent_file = fl.CSV(model=schema)

    class ChildFolder(ParentFolder):
        child_file = fl.CSV(model=schema)

    parent_source = ParentFolder.source()
    child_source = ChildFolder.source()

    parent_file = ParentFolder.schema().get_item("parent_file").unwrap()
    child_file = ChildFolder.schema().get_item("child_file").unwrap()

    # Verify path hierarchy
    _assert_path_hierarchy(parent_source, child_source, "childfolder")

    # Files are in their respective folder paths
    _assert_file_in_folder(parent_file.source, parent_source, "parent_file", "csv")
    _assert_file_in_folder(child_file.source, child_source, "child_file", "csv")

    # File paths are different
    assert str(parent_file.source) != str(child_file.source)


def test_folder_multiple_inheritance_paths(tmp_path: Path) -> None:
    """Multiple Folders in hierarchy each build unique source paths, not siblings."""
    schema = _simple_schema()

    class Root(fl.Folder):
        __source__ = Path(tmp_path)
        file1 = fl.CSV(model=schema)

    class Branch1(Root):
        file2 = fl.CSV(model=schema)

    class Branch2(Root):
        file3 = fl.CSV(model=schema)

    root_source = Root.source()
    branch1_source = Branch1.source()
    branch2_source = Branch2.source()

    # Each has different name in path
    assert "root" in str(root_source).lower()
    assert "branch1" in str(branch1_source).lower()
    assert "branch2" in str(branch2_source).lower()

    # Branch1 and Branch2 are nested under Root, not siblings
    _assert_path_hierarchy(root_source, branch1_source, "branch1")
    _assert_path_hierarchy(root_source, branch2_source, "branch2")

    # Branch1 and Branch2 are different and don't contain each other
    assert "branch1" not in str(branch2_source).lower()
    assert "branch2" not in str(branch1_source).lower()

    # All three are different paths
    paths_set = {str(root_source), str(branch1_source), str(branch2_source)}
    assert len(paths_set) == 3, f"Not all paths unique: {paths_set}"


def test_folder_deep_inheritance_hierarchy(tmp_path: Path) -> None:
    """Deep inheritance hierarchy creates proper nested paths at each level."""

    class Level1(fl.Folder):
        __source__ = Path(tmp_path)

    class Level2(Level1):
        pass

    class Level3(Level2):
        pass

    l1_source = Level1.source()
    l2_source = Level2.source()
    l3_source = Level3.source()

    # Each level is nested under previous
    _assert_path_hierarchy(l1_source, l2_source, "level2")
    _assert_path_hierarchy(l2_source, l3_source, "level3")
    # And transitively: Level1 is parent of Level3
    _assert_path_hierarchy(l1_source, l3_source, "level3")

    # All different
    assert len({str(l1_source), str(l2_source), str(l3_source)}) == 3


# ============================================================================
# Edge Cases: Special Scenarios
# ============================================================================


def test_folder_single_file_source(tmp_path: Path) -> None:
    """Folder with single file has correct source path."""
    schema = _simple_schema()

    class SingleFileFolder(fl.Folder):
        __source__ = Path(tmp_path)
        only_file = fl.CSV(model=schema)

    _assert_file_in_folder(
        SingleFileFolder.schema().get_item("only_file").unwrap().source,
        SingleFileFolder.source(),
        "only_file",
        "csv",
    )


def test_folder_case_sensitivity_in_classname(tmp_path: Path) -> None:
    """Folder class names converted to lowercase in paths."""

    class MixedCaseFolder(fl.Folder):
        __source__ = Path(tmp_path)

    source = MixedCaseFolder.source()
    assert "mixedcasefolder" in str(source).lower()
    assert str(source).endswith("mixedcasefolder")


def test_folder_empty_folder_source(tmp_path: Path) -> None:
    """Empty Folder (no files) still has valid source path."""

    class EmptyFolder(fl.Folder):
        __source__ = Path(tmp_path)

    source = EmptyFolder.source()
    assert "emptyfolder" in str(source).lower()
    tree = EmptyFolder.show_tree()
    assert "emptyfolder" in tree.lower()


def test_folder_file_source_matches_filename(tmp_path: Path) -> None:
    """File source filename matches the file attribute name."""
    schema = _simple_schema()

    class NameMatchFolder(fl.Folder):
        __source__ = Path(tmp_path)
        my_special_data = fl.Json(model=schema)

    file_name = (
        NameMatchFolder.schema().get_item("my_special_data").unwrap().source.stem
    )
    assert file_name == "my_special_data", (
        f"Expected 'my_special_data', got '{file_name}'"
    )


# ============================================================================
# Complex Inheritance Tests
# ============================================================================


def test_folder_diamond_inheritance_paths(tmp_path: Path) -> None:
    """Diamond inheritance pattern preserves correct paths for all branches."""
    schema = _simple_schema()

    class Base(fl.Folder):
        __source__ = Path(tmp_path)
        base_file = fl.Parquet(model=schema)

    class LeftBranch(Base):
        left_file = fl.CSV(model=schema)

    class RightBranch(Base):
        right_file = fl.Json(model=schema)

    class Diamond(LeftBranch, RightBranch):
        diamond_file = fl.NDJson(model=schema)

    # All files should have correct paths
    _assert_path_hierarchy(Diamond.source().parent, Diamond.source(), "diamond")

    # Check that all files from all branches are accessible
    all_files = Diamond.schema().keys()
    assert "base_file" in all_files
    assert "left_file" in all_files
    assert "right_file" in all_files
    assert "diamond_file" in all_files

    # Each file should have correct source hierarchy
    assert "diamond" in str(Diamond.base_file.source).lower()
    assert "diamond" in str(Diamond.left_file.source).lower()


def test_folder_inheritance_overrides_file_handler(tmp_path: Path) -> None:
    """Derived folder can override a file handler from parent."""

    class ParentSchema(fl.Schema):
        x = fl.Int64()

    class ChildSchema(fl.Schema):
        x = fl.Int64()
        y = fl.String()

    class ParentFolder(fl.Folder):
        __source__ = Path(tmp_path)
        data = fl.Parquet(model=ParentSchema)

    class ChildFolder(ParentFolder):
        data = fl.CSV(model=ChildSchema)  # pyright: ignore[reportIncompatibleVariableOverride]

    # Child should use CSV, not Parquet
    assert ChildFolder.data.source.suffix == ".csv"
    assert "childfolder" in str(ChildFolder.data.source).lower()


def test_folder_nested_subfolder_simulation(tmp_path: Path) -> None:
    """Simulate nested subfolders via inheritance chain."""
    schema = _simple_schema()

    class Root(fl.Folder):
        __source__ = Path(tmp_path)
        config = fl.Json(model=schema)

    class Data(Root):
        raw = fl.Parquet(model=schema)

    class Processed(Data):
        clean = fl.Parquet(model=schema)

    class Final(Processed):
        output = fl.CSV(model=schema)

    # Path should show full hierarchy
    final_path = str(Final.source())
    assert "final" in final_path.lower()

    # All parent files should be accessible
    assert "config" in Final.schema()
    assert "raw" in Final.schema()
    assert "clean" in Final.schema()
    assert "output" in Final.schema()


def test_folder_parallel_inheritance_separate_paths(tmp_path: Path) -> None:
    """Parallel inheritance creates separate path hierarchies."""
    schema = _simple_schema()

    class Common(fl.Folder):
        __source__ = Path(tmp_path)
        shared = fl.Parquet(model=schema)

    class BranchA(Common):
        file_a = fl.CSV(model=schema)

    class BranchB(Common):
        file_b = fl.Json(model=schema)

    # Each branch should have its own path
    assert "brancha" in str(BranchA.source()).lower()
    assert "branchb" in str(BranchB.source()).lower()
    assert BranchA.source() != BranchB.source()

    # Both have access to shared file (but with different paths)
    assert "shared" in BranchA.schema()
    assert "shared" in BranchB.schema()


def test_folder_mro_respects_file_collection(tmp_path: Path) -> None:
    """Method resolution order correctly collects all files from ancestors."""
    schema = _simple_schema()

    class Level0(fl.Folder):
        __source__ = Path(tmp_path)
        l0 = fl.Parquet(model=schema)

    class Level1A(Level0):
        l1a = fl.CSV(model=schema)

    class Level1B(Level0):
        l1b = fl.Json(model=schema)

    class Level2(Level1A, Level1B):
        l2 = fl.NDJson(model=schema)

    files = Level2.schema().keys()
    assert "l0" in files
    assert "l1a" in files
    assert "l1b" in files
    assert "l2" in files
    assert files.length() == 4


def test_folder_tree_shows_inheritance_hierarchy(tmp_path: Path) -> None:
    """show_tree displays all inherited files correctly."""
    schema = _simple_schema()

    class Base(fl.Folder):
        __source__ = Path(tmp_path)
        base_data = fl.Parquet(model=schema)

    class Middle(Base):
        middle_data = fl.CSV(model=schema)

    class Leaf(Middle):
        leaf_data = fl.Json(model=schema)

    tree = Leaf.show_tree()
    assert "base_data" in tree
    assert "middle_data" in tree
    assert "leaf_data" in tree


def test_folder_source_immutability_across_instances(tmp_path: Path) -> None:
    """Folder source paths are class-level and consistent."""
    schema = _simple_schema()

    class MyFolder(fl.Folder):
        __source__ = Path(tmp_path)
        data = fl.Parquet(model=schema)

    source1 = MyFolder.source()
    source2 = MyFolder.source()
    assert source1 == source2

    file_source1 = MyFolder.data.source
    file_source2 = MyFolder.data.source
    assert file_source1 == file_source2


def test_folder_with_database_entry(tmp_path: Path) -> None:
    """Folder can contain both file handlers and database entries."""
    schema = _simple_schema()

    class DBSchema(fl.Schema):
        id = fl.Int64(primary_key=True)

    class MyDB(fl.DataBase):
        table = fl.Table(model=DBSchema)

    class Project(fl.Folder):
        __source__ = Path(tmp_path)
        config = fl.Json(model=schema)
        db = MyDB()

    Project.source().mkdir(parents=True, exist_ok=True)

    # Database source should be within folder
    assert "project" in str(Project.db.source).lower()
    assert Project.db.source.suffix == ".ddb"

    # File source should also be within folder
    assert "project" in str(Project.config.source).lower()


def test_folder_complex_mixed_inheritance(tmp_path: Path) -> None:
    """Complex inheritance with mixed file types and databases."""

    class ConfigSchema(fl.Schema):
        setting = fl.String()

    class DataSchema(fl.Schema):
        id = fl.Int64(primary_key=True)
        value = fl.Float64()

    class DBConfig(fl.DataBase):
        settings = fl.Table(model=ConfigSchema)

    class BaseProject(fl.Folder):
        __source__ = Path(tmp_path)
        config = fl.Json(model=ConfigSchema)

    class DataProject(BaseProject):
        data = fl.Parquet(model=DataSchema)
        db = DBConfig()

    class FinalProject(DataProject):
        output = fl.CSV(model=DataSchema)

    FinalProject.source().mkdir(parents=True, exist_ok=True)

    # All entries should be present
    entries = FinalProject.schema().keys()
    assert "config" in entries
    assert "data" in entries
    assert "db" in entries
    assert "output" in entries

    # Paths should be correct
    assert "finalproject" in str(FinalProject.config.source).lower()
    assert "finalproject" in str(FinalProject.db.source).lower()


def test_folder_long_inheritance_chain_paths(tmp_path: Path) -> None:
    """Long inheritance chains maintain correct path structure."""
    schema = _simple_schema()

    class L1(fl.Folder):
        __source__ = Path(tmp_path)
        f1 = fl.Parquet(model=schema)

    class L2(L1):
        f2 = fl.CSV(model=schema)

    class L3(L2):
        f3 = fl.Json(model=schema)

    class L4(L3):
        f4 = fl.NDJson(model=schema)

    class L5(L4):
        f5 = fl.Parquet(model=schema)

    # Final class should have all files
    assert L5.schema().keys().length() == 5

    # Path should reflect deepest level
    assert "l5" in str(L5.source()).lower()
    assert "l5" in str(L5.f1.source).lower()
