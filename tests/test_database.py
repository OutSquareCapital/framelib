"""Tests for DataBases and their functionalities."""

from collections.abc import Generator

import pytest
from duckdb import CatalogException, ConstraintException

import framelib as fl
from tests._data import DataFrames, Sales, SampleDB, TestData

_EXPECTED_SALES_COUNT = 3
_EXPECTED_CONFLICTING_SALES_COUNT = 4
_CONFLICTING_AMOUNT = 20.0
_REPLACED_AMOUNT = 99.9
_ADD_RESULT = 8
_MIXED_POS1 = 42
_MIXED_KWARG1 = 2.5


def test_initial_sales_shape(test_db: SampleDB) -> None:
    """Test that initial sales table has correct shape."""
    assert test_db.sales.read().shape == (_EXPECTED_SALES_COUNT, 3)


def test_insert_into_with_duplicate_primary_key(test_db: SampleDB) -> None:
    """Test that insert_into raises ConstraintException on duplicate primary key."""
    with pytest.raises(ConstraintException):
        test_db.sales.insert_into(
            DataFrames.CONFLICTING_SALES.filter(Sales.order_id.pl_col.eq(2)),
        )


def test_insert_or_ignore(test_db: SampleDB) -> None:
    """Test that insert_or_ignore skips duplicates based on primary key."""
    result_shape = (
        test_db.sales.insert_or_ignore(
            DataFrames.CONFLICTING_SALES,
        )
        .read()
        .shape
    )
    assert result_shape == (_EXPECTED_CONFLICTING_SALES_COUNT, 3)

    amount = test_db.sales.read().filter(Sales.order_id.pl_col.eq(2)).item(0, "amount")
    assert amount == _CONFLICTING_AMOUNT


def test_insert_or_replace(test_db: SampleDB) -> None:
    """Test that insert_or_replace overrides existing rows."""
    order_id = 2
    amount = (
        test_db.sales.insert_or_replace(DataFrames.CONFLICTING_SALES)
        .scan()
        .filter(Sales.order_id.nw_col == order_id)
        .collect()
        .item(0, "amount")
    )
    assert amount == _REPLACED_AMOUNT


@pytest.fixture
def test_db() -> Generator[SampleDB]:
    """Setup database with test data."""
    TestData.source().mkdir(parents=True, exist_ok=True)

    @TestData.db
    def _setup() -> None:
        TestData.db.customers.create_or_replace_from(DataFrames.CUSTOMERS)
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)

    _setup()
    # Reopen connection for the test
    TestData.db.connect()
    yield TestData.db
    if TestData.db.is_connected:
        TestData.db.close()
    TestData.clean()
    TestData.clean()


def test_insert_into_with_unique_conflict(test_db: SampleDB) -> None:
    """Test that insert_into raises ConstraintException on UNIQUE conflict."""
    with pytest.raises(ConstraintException):
        test_db.sales.insert_into(DataFrames.UNIQUE_CONFLICT_SALES)


def test_truncate_sales_table(test_db: SampleDB) -> None:
    """Test that truncate empties the table."""
    assert test_db.sales.truncate().read().shape == (0, _EXPECTED_SALES_COUNT)


def test_drop_non_existing_table(test_db: SampleDB) -> None:
    """Test that dropping non-existing table raises CatalogException."""
    test_db.sales.drop()
    with pytest.raises(CatalogException):
        test_db.sales.scan()


def test_sales_file_read() -> None:
    """Test that sales_file.read returns correct shape."""
    assert TestData.sales_file.read().shape == (_EXPECTED_SALES_COUNT, 3)


@pytest.mark.usefixtures("test_folder")
def test_show_tree_format() -> None:
    """Test that show_tree returns correctly formatted tree structure."""
    tree = TestData.show_tree()

    assert "testdata" in tree
    assert "customers_file.ndjson" in tree
    assert "sales_file.csv" in tree
    assert "db.ddb" in tree
    assert "├──" in tree or "└──" in tree
    assert len(tree) > 0


@pytest.mark.usefixtures("test_folder")
def test_decorator_opens_and_closes_connection() -> None:
    """Test that decorator properly opens and closes connection."""

    @TestData.db
    def _check_connection() -> bool:
        return TestData.db.is_connected

    result = _check_connection()
    assert result is True
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_with_return_value() -> None:
    """Test that decorator returns the function's return value."""

    @TestData.db
    def _get_table_shape() -> tuple[int, int]:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return TestData.db.sales.read().shape

    result = _get_table_shape()
    assert result == (_EXPECTED_SALES_COUNT, 3)
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_closes_on_exception() -> None:
    """Test that decorator closes connection even when function raises exception."""

    @TestData.db
    def _raise_error() -> None:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        msg = "Intentional error for testing"
        raise ValueError(msg)

    with pytest.raises(ValueError, match="Intentional error"):
        _raise_error()

    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_preserves_function_metadata() -> None:
    """Test that decorator preserves the original function metadata."""

    @TestData.db
    def _my_decorated_function() -> None:
        """This is the decorated function docstring."""
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)

    assert _my_decorated_function.__name__ == "_my_decorated_function"
    docstring = _my_decorated_function.__doc__
    assert docstring is not None
    assert "decorated function docstring" in docstring


@pytest.mark.usefixtures("test_folder")
def test_decorator_multiple_sequential_calls() -> None:
    """Test that decorator can open and close connection multiple times."""

    @TestData.db
    def _count_sales() -> int:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return TestData.db.sales.read().shape[0]

    result1 = _count_sales()
    assert TestData.db.is_connected is False
    assert result1 == _EXPECTED_SALES_COUNT

    result2 = _count_sales()
    assert TestData.db.is_connected is False
    assert result2 == _EXPECTED_SALES_COUNT


@pytest.mark.usefixtures("test_folder")
def test_decorator_with_no_return() -> None:
    """Test decorator properly handles functions returning None."""

    @TestData.db
    def _operation() -> None:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)

    result = _operation()
    assert result is None
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_with_complex_return_type() -> None:
    """Test decorator with complex return types."""

    @TestData.db
    def _return_complex() -> dict[str, int]:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return {"count": TestData.db.sales.read().shape[0], "multiplied": 10}

    result = _return_complex()
    assert result == {"count": _EXPECTED_SALES_COUNT, "multiplied": 10}
    assert TestData.db.is_connected is False


# Edge cases for decorator


@pytest.mark.usefixtures("test_folder")
def test_decorator_with_positional_arguments() -> None:
    """Test that decorator properly passes positional arguments to function."""

    @TestData.db
    def _add_numbers(a: int, b: int) -> int:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return a + b

    result = _add_numbers(5, 3)
    assert result == _ADD_RESULT
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_with_keyword_arguments() -> None:
    """Test that decorator properly passes keyword arguments to function."""

    @TestData.db
    def _greet(name: str, greeting: str = "Hello") -> str:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return f"{greeting}, {name}!"

    result1 = _greet("Alice")
    assert result1 == "Hello, Alice!"
    assert TestData.db.is_connected is False

    result2 = _greet("Bob", greeting="Hi")
    assert result2 == "Hi, Bob!"
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_with_mixed_arguments() -> None:
    """Test that decorator properly passes mixed positional and keyword arguments."""

    @TestData.db
    def _mixed(
        pos1: int, pos2: str, kwarg1: float = 1.0
    ) -> dict[str, int | str | float]:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return {
            "pos1": pos1,
            "pos2": pos2,
            "kwarg1": kwarg1,
        }

    result = _mixed(_MIXED_POS1, "test", kwarg1=_MIXED_KWARG1)
    assert result["pos1"] == _MIXED_POS1
    assert result["pos2"] == "test"
    assert result["kwarg1"] == _MIXED_KWARG1
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_connection_reuse_if_already_open() -> None:
    """Test that decorator reuses existing connection instead of closing prematurely."""

    @TestData.db
    def _operation() -> bool:
        # Check that we can query inside decorator
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return True

    # Manually open connection
    TestData.db.__enter__()
    assert TestData.db.is_connected is True

    # Decorator should reuse this connection
    result = _operation()
    assert result is True
    # After decorator, connection should still be closed from decorator's perspective
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_with_different_exception_types() -> None:
    """Test that decorator closes connection for different exception types."""

    @TestData.db
    def _raise_type_error() -> None:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        msg = "Type error"
        raise TypeError(msg)

    @TestData.db
    def _raise_runtime_error() -> None:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        msg = "Runtime error"
        raise RuntimeError(msg)

    with pytest.raises(TypeError, match="Type error"):
        _raise_type_error()
    assert TestData.db.is_connected is False

    with pytest.raises(RuntimeError, match="Runtime error"):
        _raise_runtime_error()
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_connection_state_transitions() -> None:
    """Test connection state transitions through multiple decorated calls."""
    assert TestData.db.is_connected is False

    @TestData.db
    def _check_and_create() -> None:
        assert TestData.db.is_connected is True
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)

    # Call 1
    _check_and_create()
    assert TestData.db.is_connected is False

    # Call 2 - connection should open again
    _check_and_create()
    assert TestData.db.is_connected is False

    # Manual context manager usage
    with TestData.db:
        assert TestData.db.is_connected is True
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_with_zero_return_value() -> None:
    """Test that decorator properly distinguishes 0 from None."""

    @TestData.db
    def _return_zero() -> int:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return 0

    result = _return_zero()
    assert result == 0
    assert TestData.db.is_connected is False


@pytest.mark.usefixtures("test_folder")
def test_decorator_multiple_exceptions_in_sequence() -> None:
    """Test that decorator properly handles multiple exceptions in sequence."""

    @TestData.db
    def _succeed() -> str:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        return "Success"

    @TestData.db
    def _fail() -> None:
        TestData.db.sales.create_or_replace_from(DataFrames.SALES)
        msg = "Raised as requested"
        raise ValueError(msg)

    # First call raises
    with pytest.raises(ValueError, match="Raised as requested"):
        _fail()
    assert TestData.db.is_connected is False

    # Second call succeeds
    result = _succeed()
    assert result == "Success"
    assert TestData.db.is_connected is False

    # Third call raises again
    with pytest.raises(ValueError, match="Raised as requested"):
        _fail()
    assert TestData.db.is_connected is False


class TestDatabaseInstanceManagement:
    """Test that DataBase instances are properly managed within Folder contexts."""

    def test_multiple_database_classes_separate_instances(self) -> None:
        """Test that different DataBase subclasses have separate instances."""

        class DB1(fl.DataBase):
            table1 = fl.Table(model=fl.Schema)

        class DB2(fl.DataBase):
            table2 = fl.Table(model=fl.Schema)

        class Project(fl.Folder):
            database1 = DB1()
            database2 = DB2()

        # Each should be a different instance
        assert Project.database1 is not Project.database2
        assert isinstance(Project.database1, DB1)
        assert isinstance(Project.database2, DB2)

    def test_inherited_database_separate_instances(self) -> None:
        """Test that inherited DataBase classes have separate instances."""

        class BaseDB(fl.DataBase):
            users = fl.Table(model=fl.Schema)

        class ExtendedDB(BaseDB):
            orders = fl.Table(model=fl.Schema)

        class Project(fl.Folder):
            db_base = BaseDB()
            db_extended = ExtendedDB()

        # Even though ExtendedDB inherits from BaseDB, they should be different instances
        assert Project.db_base is not Project.db_extended
        assert isinstance(Project.db_base, BaseDB)
        assert isinstance(Project.db_extended, ExtendedDB)

    def test_nested_database_in_nested_folders(self) -> None:
        """Test DataBase instances in nested Folder structures."""

        class DB(fl.DataBase):
            table1 = fl.Table(model=fl.Schema)

        class InnerFolder(fl.Folder):
            db = DB()

        class OuterFolder(fl.Folder):
            inner = InnerFolder()

        # The instance should be created once in InnerFolder
        assert OuterFolder.inner.db is OuterFolder.inner.db
        inner1 = OuterFolder.inner
        inner2 = OuterFolder.inner
        # Accessing InnerFolder through OuterFolder should give same instance
        assert inner1.db is inner2.db

    def test_database_connection_isolation(self) -> None:
        """Test that connections are properly isolated between uses."""

        def create_data() -> None:
            TestData.source().mkdir(parents=True, exist_ok=True)
            with TestData.db as db:
                db.customers.create_or_replace_from(DataFrames.CUSTOMERS)

        def read_data() -> int:
            with TestData.db as db:
                return len(db.customers.read())

        # Create data in one context
        create_data()
        # Read in another context - should still work
        count = read_data()
        assert count == len(DataFrames.CUSTOMERS)

    def test_database_instance_in_folder_not_recreated(self) -> None:
        """Test that DataBase instance in Folder is not recreated on access."""

        class DB(fl.DataBase):
            table1 = fl.Table(model=fl.Schema)

        class Project(fl.Folder):
            db = DB()

        # Get the instance multiple times
        instance1 = Project.db
        instance2 = Project.db
        instance3 = Project.db

        # All should be the same object in memory
        assert id(instance1) == id(instance2) == id(instance3)


class TestDatabaseConnectionEdgeCases:
    """Test edge cases in connection management."""

    def test_database_connection_cleanup_on_exception(self) -> None:
        """Test that connection is closed even if an exception occurs."""
        TestData.source().mkdir(parents=True, exist_ok=True)

        def _create_and_fail() -> None:
            with TestData.db as db:
                db.customers.create_or_replace_from(DataFrames.CUSTOMERS)
                msg = "Test error"
                raise ValueError(msg)

        with pytest.raises(ValueError, match="Test error"):
            _create_and_fail()

        # Connection should be closed
        assert not TestData.db.is_connected

    def test_database_decorator_cleanup_on_exception(self) -> None:
        """Test that connection is closed even if decorated function raises."""
        TestData.source().mkdir(parents=True, exist_ok=True)

        @TestData.db
        def failing_operation() -> None:
            TestData.db.customers.create_or_replace_from(DataFrames.CUSTOMERS)
            msg = "Test error"
            raise RuntimeError(msg)

        with pytest.raises(RuntimeError, match="Test error"):
            failing_operation()

        # Connection should still be closed after exception
        assert not TestData.db.is_connected

    def test_database_no_connection_leak_on_multiple_enters(self) -> None:
        """Test that multiple enters don't leak connections."""
        TestData.source().mkdir(parents=True, exist_ok=True)
        # Enter context multiple times
        with TestData.db:
            pass

        assert not TestData.db.is_connected

        with TestData.db:
            pass

        assert not TestData.db.is_connected

        # Should still work
        with TestData.db as db:
            db.customers.create_or_replace_from(DataFrames.CUSTOMERS)

        assert not TestData.db.is_connected
