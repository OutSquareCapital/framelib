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


# Tests for @DataBase.connect decorator

EXPECTED_SALES_COUNT = 3
EXPECTED_FILTERED_COUNT = 2
MULTIPLIED_RESULT = 6


def test_connect_decorator_opens_connection(test_folder: None) -> None:  # noqa: ARG001
    """Test that @connect decorator properly opens the database connection."""

    @TestData.db.connect
    def _check_connection(db: TestDB) -> bool:
        return db.is_connected

    assert _check_connection(TestData.db) is True


def test_connect_decorator_closes_connection(test_folder: None) -> None:  # noqa: ARG001
    """Test that @connect decorator properly closes the connection after execution."""

    @TestData.db.connect
    def _check_then_return(db: TestDB) -> None:
        assert db.is_connected is True

    # Call the decorated function
    _check_then_return(TestData.db)

    # Verify connection is closed after decorator exits
    assert TestData.db.is_connected is False


def test_connect_decorator_with_return_value(test_folder: None) -> None:  # noqa: ARG001
    """Test that @connect decorator properly returns the function's return value."""

    @TestData.db.connect
    def _get_table_shape(db: TestDB) -> tuple[int, int]:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return db.sales.read().shape

    result = _get_table_shape(TestData.db)

    assert result == (3, 3)
    assert TestData.db.is_connected is False


def test_connect_decorator_with_function_args(test_folder: None) -> None:  # noqa: ARG001
    """Test that @connect decorator properly passes additional function arguments."""

    @TestData.db.connect
    def _insert_and_count(db: TestDB, multiplier: int) -> int:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return db.sales.read().shape[0] * multiplier

    result = _insert_and_count(TestData.db, multiplier=2)

    assert result == MULTIPLIED_RESULT
    assert TestData.db.is_connected is False


def test_connect_decorator_with_function_kwargs(test_folder: None) -> None:  # noqa: ARG001
    """Test that @connect decorator properly passes keyword arguments."""

    @TestData.db.connect
    def _filter_by_amount(db: TestDB, min_amount: float = 0.0) -> int:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return (
            db.sales.scan().filter(Sales.amount.nw_col >= min_amount).collect().shape[0]
        )

    result_all = _filter_by_amount(TestData.db, min_amount=0.0)
    result_filtered = _filter_by_amount(TestData.db, min_amount=20.0)

    assert result_all == EXPECTED_SALES_COUNT
    assert result_filtered == EXPECTED_FILTERED_COUNT
    assert TestData.db.is_connected is False


def test_connect_decorator_closes_on_exception(test_folder: None) -> None:  # noqa: ARG001
    """Test that @connect decorator closes connection even when function raises exception."""

    @TestData.db.connect
    def _raise_error(db: TestDB) -> None:
        db.sales.create_or_replace_from(DataFrames.SALES)
        msg = "Intentional error for testing"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="Intentional error"):
        _raise_error(TestData.db)

    # Verify connection is closed even after exception
    assert TestData.db.is_connected is False


def test_connect_decorator_preserves_function_metadata(test_folder: None) -> None:  # noqa: ARG001
    """Test that @connect decorator preserves the original function metadata."""

    @TestData.db.connect
    def _my_decorated_function(db: TestDB) -> None:
        """This is the decorated function docstring."""
        db.sales.create_or_replace_from(DataFrames.SALES)

    assert _my_decorated_function.__name__ == "_my_decorated_function"
    docstring = _my_decorated_function.__doc__
    assert docstring is not None
    assert "decorated function docstring" in docstring


# Additional edge case tests for @DataBase.connect decorator

EDGE_CASE_COUNT = 5


@pytest.mark.usefixtures("test_folder")
def test_connect_decorator_multiple_sequential_calls() -> None:
    """Test that decorator can open and close connection multiple times."""

    @TestData.db.connect
    def _increment_counter(db: TestDB) -> int:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return db.sales.read().shape[0]

    # Call multiple times sequentially
    result1 = _increment_counter(TestData.db)
    assert TestData.db.is_connected is False

    result2 = _increment_counter(TestData.db)
    assert TestData.db.is_connected is False

    assert result1 == EXPECTED_SALES_COUNT
    assert result2 == EXPECTED_SALES_COUNT


@pytest.mark.usefixtures("test_folder")
def test_connect_decorator_returns_none() -> None:
    """Test that decorator properly handles functions returning None."""

    @TestData.db.connect
    def _operation_with_no_return(db: TestDB) -> None:
        db.sales.create_or_replace_from(DataFrames.SALES)

    result = _operation_with_no_return(TestData.db)

    assert result is None
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_connect_decorator_returns_explicit_none() -> None:
    """Test that decorator properly handles explicit None returns."""

    @TestData.db.connect
    def _explicit_none_return(db: TestDB) -> None:
        db.sales.create_or_replace_from(DataFrames.SALES)

    result = _explicit_none_return(TestData.db)

    assert result is None
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_connect_decorator_with_mixed_args_and_kwargs() -> None:
    """Test that decorator properly passes both positional and keyword arguments."""

    @TestData.db.connect
    def _mixed_args(
        db: TestDB, pos_arg: int, /, keyword_arg: float = 1.0
    ) -> tuple[int, float]:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return (pos_arg, keyword_arg)

    result = _mixed_args(TestData.db, EDGE_CASE_COUNT, keyword_arg=2.5)

    assert result == (EDGE_CASE_COUNT, 2.5)
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_connect_decorator_no_additional_args() -> None:
    """Test decorator when function only takes db as argument."""

    @TestData.db.connect
    def _only_db_arg(db: TestDB) -> int:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return db.sales.read().shape[0]

    result = _only_db_arg(TestData.db)

    assert result == EXPECTED_SALES_COUNT
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_connect_decorator_nested_decorated_calls() -> None:
    """Test calling one decorated function from another decorated function."""

    @TestData.db.connect
    def _inner_decorated(db: TestDB) -> str:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return "inner"

    @TestData.db.connect
    def _outer_decorated(_db: TestDB) -> str:
        # Note: This calls the decorated function, not the raw one
        # So it will try to use db again, which is already connected
        result = _inner_decorated(TestData.db)
        return f"outer_{result}"

    # This should work but reveals connection state during nested call
    _outer_decorated(TestData.db)

    # Connection should be closed after outer exits
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_connect_decorator_with_complex_return_type() -> None:
    """Test decorator with complex return types like dict/list."""

    @TestData.db.connect
    def _return_complex(db: TestDB) -> dict[str, int]:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return {"count": db.sales.read().shape[0], "multiplied": 10}

    result = _return_complex(TestData.db)

    assert result == {"count": EXPECTED_SALES_COUNT, "multiplied": 10}
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_connect_decorator_database_state_isolation() -> None:
    """Test that database changes are isolated between decorated calls."""

    @TestData.db.connect
    def _setup_and_count(db: TestDB) -> int:
        db.sales.create_or_replace_from(DataFrames.SALES)
        return db.sales.read().shape[0]

    @TestData.db.connect
    def _count_only(db: TestDB) -> int:
        return db.sales.read().shape[0]

    # First call sets up data
    count1 = _setup_and_count(TestData.db)
    assert count1 == EXPECTED_SALES_COUNT

    # Second call should still see the data (persisted in db file)
    count2 = _count_only(TestData.db)
    assert count2 == EXPECTED_SALES_COUNT
    assert TestData.db.is_connected is False
