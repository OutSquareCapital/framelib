"""Tests for Folder functionalities and structure."""

from collections.abc import Generator
from pathlib import Path

import pytest

from tests._data import DataFrames, TestData


@pytest.fixture
def test_folder() -> Generator[None]:
    """Create test folder structure."""
    TestData.source().mkdir(parents=True, exist_ok=True)
    TestData.sales_file.write(DataFrames.SALES)
    yield
    if TestData.db.is_connected:
        TestData.db.close()
    import shutil

    shutil.rmtree(TestData.source())


class TestFolderStructure:
    """Test Folder class structure and behavior."""

    def test_folder_source_path(self) -> None:
        """Test that Folder correctly sets source path."""
        assert str(TestData.source()) == str(Path("testdata"))
        assert TestData.source().name == "testdata"

    def test_folder_schema_contains_entries(self) -> None:
        """Test that Folder schema contains all defined entries."""
        schema = TestData.schema()
        assert "sales_file" in schema
        assert "customers_file" in schema
        assert "db" in schema

    def test_folder_get_entries(self) -> None:
        """Test accessing Folder entries."""
        assert TestData.sales_file is not None
        assert TestData.customers_file is not None
        assert TestData.db is not None

    def test_folder_entries_have_correct_types(self) -> None:
        """Test that Folder entries have correct types."""
        import framelib as fl

        assert isinstance(TestData.sales_file, fl.CSV)
        assert isinstance(TestData.customers_file, fl.NDJson)
        assert isinstance(TestData.db, fl.DataBase)

    @pytest.mark.usefixtures("test_folder")
    def test_folder_shows_tree(self) -> None:
        """Test that show_tree generates correct tree structure."""
        tree = TestData.show_tree()

        # Check for folder name
        assert "testdata" in tree.lower()

        # Check for files
        assert "sales_file.csv" in tree
        assert "customers_file.ndjson" in tree
        assert "db.ddb" in tree

        # Check for tree characters
        assert "├──" in tree or "└──" in tree

    @pytest.mark.usefixtures("test_folder")
    def test_folder_file_paths_computed_correctly(self) -> None:
        """Test that file paths are computed correctly."""
        sales_path = TestData.sales_file.source
        customers_path = TestData.customers_file.source

        assert sales_path == Path("tests") / "sales_file.csv"
        assert customers_path == Path("tests") / "customers_file.ndjson"


class TestNestedFolders:
    """Test nested Folder structures."""

    def test_nested_folders_create_hierarchy(self) -> None:
        """Test that nested folders create correct hierarchy."""
        import framelib as fl

        class Inner(fl.Folder):
            file1 = fl.CSV()

        class Outer(fl.Folder):
            inner = Inner()

        # Access nested folder
        assert Outer.inner is not None
        assert isinstance(Outer.inner, fl.Folder)

    def test_nested_folders_compute_paths(self) -> None:
        """Test that nested folders compute paths correctly."""
        import framelib as fl

        class Inner(fl.Folder):
            data = fl.Parquet()

        class Outer(fl.Folder):
            nested = Inner()

        # Inner folder path should be under Outer's path
        inner_path = Outer.nested.source()
        outer_path = Outer.source()

        assert str(inner_path).endswith("outer/inner")
        assert "outer" in str(outer_path)


class TestFolderWithDatabase:
    """Test Folder containing Database."""

    @pytest.mark.usefixtures("test_folder")
    def test_folder_with_database_instance(self) -> None:
        """Test that Folder can contain Database instance."""
        assert TestData.db is not None
        assert hasattr(TestData, "db")

    @pytest.mark.usefixtures("test_folder")
    def test_folder_database_in_tree(self) -> None:
        """Test that Database appears in folder tree."""
        tree = TestData.show_tree()
        assert "db.ddb" in tree

    def test_folder_database_singleton_per_folder(self) -> None:
        """Test that each Folder instance has its own Database instance."""
        import framelib as fl

        class DB1(fl.DataBase):
            table1 = fl.Table(model=fl.Schema)

        class MyFolder1(fl.Folder):
            db = DB1()

        class MyFolder2(fl.Folder):
            db = DB1()

        # Each folder should have the same DB instance
        # because it's the same class attribute
        assert MyFolder1.db is MyFolder2.db
