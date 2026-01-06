from collections.abc import Generator

import pytest
from duckdb import CatalogException, ConstraintException

from tests._data import DataFrames, Sales, TestData, TestDB


@pytest.fixture
def test_folder() -> Generator[None]:
    """Create test folder structure."""
    TestData.source().mkdir(parents=True, exist_ok=True)
    TestData.sales_file.write(DataFrames.SALES)
    print(TestData.show_tree())
    yield
    if TestData.db.is_connected:
        TestData.db.close()
    TestData.clean()


@pytest.fixture
def test_db(test_folder: None) -> TestDB:  # noqa: ARG001
    """Setup database with test data."""

    def _setup(db: TestDB) -> None:
        db.customers.create_or_replace_from(DataFrames.CUSTOMERS)
        db.sales.create_or_replace_from(DataFrames.SALES)

    TestData.db.apply(_setup)
    return TestData.db


def test_initial_sales_shape(test_db: TestDB) -> None:
    """Test that initial sales table has correct shape."""
    assert test_db.sales.read().shape == (3, 3)


def test_insert_into_with_duplicate_primary_key(test_db: TestDB) -> None:
    """Test that insert_into raises ConstraintException on duplicate primary key."""
    with pytest.raises(ConstraintException):
        test_db.sales.insert_into(
            DataFrames.CONFLICTING_SALES.filter(Sales.order_id.pl_col.eq(2)),
        )


def test_insert_or_ignore(test_db: TestDB) -> None:
    """Test that insert_or_ignore skips duplicates based on primary key."""
    result_shape = (
        test_db.sales.insert_or_ignore(
            DataFrames.CONFLICTING_SALES,
        )
        .read()
        .shape
    )
    assert result_shape == (4, 3)

    amount = test_db.sales.read().filter(Sales.order_id.pl_col.eq(2)).item(0, "amount")
    assert amount == 20.0  # noqa: PLR2004


def test_insert_or_replace(test_db: TestDB) -> None:
    """Test that insert_or_replace overrides existing rows."""
    order_id = 2
    amount = (
        test_db.sales.insert_or_replace(DataFrames.CONFLICTING_SALES)
        .scan()
        .filter(Sales.order_id.nw_col == order_id)
        .collect()
        .item(0, "amount")
    )
    assert amount == 99.9  # noqa: PLR2004


def test_insert_into_with_unique_conflict(test_db: TestDB) -> None:
    """Test that insert_into raises ConstraintException on UNIQUE conflict."""
    with pytest.raises(ConstraintException):
        test_db.sales.insert_into(DataFrames.UNIQUE_CONFLICT_SALES)


def test_truncate_sales_table(test_db: TestDB) -> None:
    """Test that truncate empties the table."""
    assert test_db.sales.truncate().read().shape == (0, 3)


def test_drop_non_existing_table(test_db: TestDB) -> None:
    """Test that dropping non-existing table raises CatalogException."""
    test_db.sales.drop()
    with pytest.raises(CatalogException):
        test_db.sales.scan()


def test_sales_file_read(test_db: TestDB) -> None:  # noqa: ARG001
    """Test that sales_file.read returns correct shape."""
    assert TestData.sales_file.read().shape == (3, 3)


def test_show_tree_format(test_folder: None) -> None:  # noqa: ARG001
    """Test that show_tree returns correctly formatted tree structure."""
    tree = TestData.show_tree()

    # Verify root path is present
    assert "testdata" in tree

    # Verify tree contains expected files
    assert "customers_file.ndjson" in tree
    assert "sales_file.csv" in tree
    assert "db.ddb" in tree

    # Verify tree structure markers are present
    assert "├──" in tree or "└──" in tree

    # Verify output is not empty
    assert len(tree) > 0
