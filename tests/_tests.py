from collections.abc import Callable
from typing import TYPE_CHECKING

import pyochain as pc
from duckdb import CatalogException, ConstraintException

from tests._data import DataFrames, Sales, TestData, TestDB

if TYPE_CHECKING:
    import polars as pl

TestResult = pc.Result[None, str]


def from_callable[T, E](
    fn: Callable[[], T],
    map_err: Callable[[BaseException], E],
    *exc_types: type[BaseException],
) -> pc.Result[T, E]:
    try:
        return pc.Ok(fn())
    except exc_types as exc:
        return pc.Err(map_err(exc))


def expect_equal[T](actual: T, expected: T, ctx: str) -> TestResult:
    if actual == expected:
        return pc.Ok(None)
    msg = f"{ctx}: expected {expected!r}, got {actual!r}"
    return pc.Err(msg)


def expect_raises[E: BaseException](
    exc_type: type[E],
    fn: Callable[[], object],
    ctx: str,
) -> TestResult:
    try:
        fn()
    except exc_type:
        return pc.Ok(None)
    except Exception as e:
        msg = f"{ctx}: expected {exc_type.__name__}, got {type(e).__name__}: {e}"
        return pc.Err(msg)
    else:
        msg = f"{ctx}: expected {exc_type.__name__}, but no exception was raised"
        return pc.Err(msg)


def setup_folder() -> None:
    TestData.source().mkdir(parents=True, exist_ok=True)
    TestData.sales_file.write(DataFrames.SALES)
    print(TestData.show_tree())


def setup_test_data(db: TestDB) -> TestResult:
    try:
        db.customers.create_or_replace_from(DataFrames.CUSTOMERS)
        db.sales.create_or_replace_from(DataFrames.SALES)
        TestData.show_tree()
        return pc.Ok(None)
    except Exception as e:
        return pc.Err(f"setup_test_data failed: {e!r}")


def teardown_test_data() -> TestResult:
    try:
        TestData.db.close()
        TestData.clean()
        return pc.Ok(None)
    except Exception as e:
        return pc.Err(f"teardown_test_data failed: {e!r}")


def test_file_operations() -> TestResult:
    df: pl.DataFrame = TestData.sales_file.read_cast()
    return expect_equal(df.shape, (3, 3), "test_file_operations: sales_file.read_cast")


def test_db_op(db: TestDB) -> TestResult:
    def step_initial_shape() -> TestResult:
        df: pl.DataFrame = db.sales.read()
        return expect_equal(df.shape, (3, 3), "sales.read initial shape")

    def step_describe_columns() -> TestResult:
        db.sales.describe_columns().collect("polars")
        return pc.Ok(None)

    def step_insert_conflicting_pk_fails() -> TestResult:
        return expect_raises(
            ConstraintException,
            lambda: db.sales.insert_into(
                DataFrames.CONFLICTING_SALES.filter(Sales.order_id.pl_col.eq(2)),
            ),
            "insert_into with duplicate primary key must fail",
        )

    def step_insert_or_ignore() -> TestResult:
        result: pl.DataFrame = db.sales.insert_or_ignore(
            DataFrames.CONFLICTING_SALES,
        ).read()

        return expect_equal(
            result.shape,
            (4, 3),
            "insert_or_ignore result shape",
        ).and_then(
            lambda _: expect_equal(
                result.filter(Sales.order_id.pl_col.eq(2)).item(0, "amount"),
                20.0,
                "insert_or_ignore keeps existing amount for order_id=2",
            ),
        )

    def step_insert_or_replace() -> TestResult:
        order_id = 2
        df = (
            db.sales.insert_or_replace(DataFrames.CONFLICTING_SALES)
            .scan()
            .filter(Sales.order_id.nw_col == order_id)
            .collect()
        )

        return expect_equal(
            df.item(0, "amount"),
            99.9,
            f"insert_or_replace overrides amount for order_id={order_id}",
        )

    def step_unique_conflict_insert_fails() -> TestResult:
        return expect_raises(
            ConstraintException,
            lambda: db.sales.insert_into(DataFrames.UNIQUE_CONFLICT_SALES),
            "insert_into with UNIQUE conflict must fail",
        )

    def step_truncate() -> TestResult:
        df: pl.DataFrame = db.sales.truncate().read()
        return expect_equal(df.shape, (0, 3), "truncate sales table")

    def step_drop_table() -> TestResult:
        return expect_raises(
            CatalogException,
            lambda: db.sales.drop().scan(),
            "drop non-existing table must raise CatalogException",
        )

    return (
        step_initial_shape()
        .and_then(lambda _: step_describe_columns())
        .and_then(lambda _: step_insert_conflicting_pk_fails())
        .and_then(lambda _: step_insert_or_ignore())
        .and_then(lambda _: step_insert_or_replace())
        .and_then(lambda _: step_unique_conflict_insert_fails())
        .and_then(lambda _: step_truncate())
        .and_then(lambda _: step_drop_table())
    )


def run_tests() -> None:
    print("🚀 Démarrage des tests de framelib...")

    result: TestResult = pc.Err("no result")
    try:
        result = (
            from_callable(
                setup_folder,
                lambda e: f"setup_folder failed: {e!r}",
                OSError,
            )
            .map(lambda _: None)
            .and_then(
                lambda _: TestData.db.apply(setup_test_data).pipe(
                    test_db_op,
                ),
            )
            .and_then(lambda _: test_file_operations())
        )
    except Exception as e:
        result = pc.Err(f"unhandled exception during tests: {e!r}")
    finally:
        teardown_result = teardown_test_data()
        match (result, teardown_result):
            case pc.Ok(_), pc.Ok(_):
                print("\n🎉 Tous les tests sont passés avec succès!")
            case pc.Ok(_), pc.Err(err2):
                msg = f"❌ ERREUR PENDANT LES TESTS:\nteardown failed: {err2}"
                raise ValueError(msg)
            case pc.Err(err), pc.Ok(_):
                msg = f"❌ ERREUR PENDANT LES TESTS:\n{err}"
                raise ValueError(msg)
            case pc.Err(err), pc.Err(err2):
                msg = f"❌ ERREUR PENDANT LES TESTS:\n{err}\n(plus teardown failed: {err2})"
                raise ValueError(msg)
            case _:
                msg = "❌ ERREUR INATTENDUE PENDANT LES TESTS"
                raise ValueError(msg)
