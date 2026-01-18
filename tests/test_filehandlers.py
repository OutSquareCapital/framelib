"""Test for filehandlers."""

from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl

import framelib as fl
from tests._data import DataFrames, Sales


class TestParquetFileHandler:
    """Test Parquet file handler."""

    def test_parquet_write_and_read(self) -> None:
        """Test writing and reading Parquet files."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.Parquet(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Read data
            result = MyFolder.data.read()

            # Verify
            assert result.shape == DataFrames.SALES.shape
            assert list(result.columns) == list(DataFrames.SALES.columns)

    def test_parquet_scan_lazy(self) -> None:
        """Test scanning Parquet file lazily."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.Parquet(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Scan lazily
            lazy_frame = MyFolder.data.scan()
            result = lazy_frame.collect()

            # Verify
            assert result.shape == DataFrames.SALES.shape

    def test_parquet_with_schema(self) -> None:
        """Test that schema is applied when reading Parquet."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.Parquet(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Read with schema validation
            result = MyFolder.data.read()

            # Verify schema
            assert result.schema == Sales.to_pl()


class TestCSVFileHandler:
    """Test CSV file handler."""

    def test_csv_write_and_read(self) -> None:
        """Test writing and reading CSV files."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.CSV(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Read data
            result = MyFolder.data.read()

            # Verify
            assert result.shape == DataFrames.SALES.shape

    def test_csv_scan_lazy(self) -> None:
        """Test scanning CSV file lazily."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.CSV(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Scan lazily
            lazy_frame = MyFolder.data.scan()
            result = lazy_frame.collect()

            # Verify
            assert result.shape == DataFrames.SALES.shape


class TestNDJsonFileHandler:
    """Test NDJSON file handler."""

    def test_ndjson_write_and_read(self) -> None:
        """Test writing and reading NDJSON files."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.NDJson(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Read data
            result = MyFolder.data.read()

            # Verify
            assert result.shape == DataFrames.SALES.shape

    def test_ndjson_scan_lazy(self) -> None:
        """Test scanning NDJSON file lazily."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.NDJson(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Scan lazily
            lazy_frame = MyFolder.data.scan()
            result = lazy_frame.collect()

            # Verify
            assert result.shape == DataFrames.SALES.shape


class TestJsonFileHandler:
    """Test JSON file handler."""

    def test_json_write_and_read(self) -> None:
        """Test writing and reading JSON files."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.Json(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Read data
            result = MyFolder.data.read()

            # Verify
            assert result.shape == DataFrames.SALES.shape

    def test_json_scan_lazy(self) -> None:
        """Test scanning JSON file lazily with DuckDB backend."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.Json(model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Scan lazily
            lazy_frame = MyFolder.data.scan()
            result = lazy_frame.collect()

            # Verify
            assert result.shape == DataFrames.SALES.shape


class TestParquetPartitionedFileHandler:
    """Test partitioned Parquet file handler."""

    def test_partitioned_parquet_write_and_read(self) -> None:
        """Test writing and reading partitioned Parquet files."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.ParquetPartitioned(partition_by=["order_id"], model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Read data
            result = MyFolder.data.read()

            # Verify
            assert result.shape == DataFrames.SALES.shape

    def test_partitioned_parquet_creates_directory_structure(self) -> None:
        """Test that partitioned Parquet creates directory structure."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.ParquetPartitioned(partition_by=["order_id"], model=Sales)

            # Write data
            MyFolder.data.write(DataFrames.SALES)

            # Check directory structure
            data_path = MyFolder.data.source
            assert data_path.is_dir()

            # Should have subdirectories for partitions
            subdirs = list(data_path.glob("order_id=*"))
            assert len(subdirs) > 0


class TestFilePathComputation:
    """Test that file paths are computed correctly."""

    def test_file_inherits_folder_path(self) -> None:
        """Test that file path is computed from folder path."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                my_data = fl.CSV()

            expected_path = Path(tmpdir) / "myfolder" / "my_data.csv"
            assert MyFolder.my_data.source == expected_path

    def test_multiple_files_different_paths(self) -> None:
        """Test that multiple files get different paths."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data1 = fl.CSV()
                data2 = fl.Parquet()
                data3 = fl.NDJson()

            assert MyFolder.data1.source != MyFolder.data2.source
            assert MyFolder.data2.source != MyFolder.data3.source
            assert MyFolder.data3.source != MyFolder.data1.source


class TestFileWithSchema:
    """Test file handlers with schema validation."""

    def test_file_with_schema_validates_on_read(self) -> None:
        """Test that schema is validated when reading file."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                sales = fl.CSV(model=Sales)

            # Write data
            MyFolder.sales.write(DataFrames.SALES)

            # Read with schema
            result = MyFolder.sales.read()

            # Schema should match
            assert result.schema == Sales.to_pl()

    def test_file_without_schema(self) -> None:
        """Test that file without schema works fine."""
        with TemporaryDirectory() as tmpdir:

            class MyFolder(fl.Folder):
                __source__ = Path(tmpdir)
                data = fl.CSV()

            # Create simple CSV
            df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            MyFolder.data.write(df)

            # Read without schema
            result = MyFolder.data.read()

            # Should work fine
            assert result.shape == (3, 2)
