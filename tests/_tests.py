from collections.abc import Callable

import pyochain as pc
from duckdb import CatalogException, ConstraintException

from tests._data import DataFrames, Sales, TestData, TestDB, setup_folder

TestResult = pc.Result[str, str]


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
        return pc.Ok(ctx)
    msg = f"{ctx}: expected {expected}, got {actual}"
    return pc.Err(msg)


def expect_raises[E: BaseException](
    exc_type: type[E],
    fn: Callable[[], object],
    ctx: str,
) -> TestResult:
    try:
        fn()
    except exc_type:
        return pc.Ok(fn.__name__)
    except Exception as e:
        msg = f"{ctx}: expected {exc_type.__name__}, got {type(e).__name__}: {e}"
        return pc.Err(msg)
    else:
        msg = f"{ctx}: expected {exc_type.__name__}, but no exception was raised"
        return pc.Err(msg)


def setup_test_data(db: TestDB) -> TestResult:
    try:
        db.customers.create_or_replace_from(DataFrames.CUSTOMERS)
        db.sales.create_or_replace_from(DataFrames.SALES)
        TestData.show_tree()
        return pc.Ok("setup_test_data succeeded")
    except Exception as e:
        return pc.Err(f"setup_test_data failed: {e}")


def teardown_test_data() -> TestResult:
    try:
        TestData.db.close()
        TestData.clean()
        return pc.Ok("teardown_test_data succeeded")
    except Exception as e:
        return pc.Err(f"teardown_test_data failed: {e}")


def test_db_op(db: TestDB) -> TestResult:
    order_id = 2

    return (
        expect_equal(db.sales.read().shape, (3, 3), "sales.read initial shape")
        .and_then(
            lambda _: expect_raises(
                ConstraintException,
                lambda: db.sales.insert_into(
                    DataFrames.CONFLICTING_SALES.filter(Sales.order_id.pl_col.eq(2)),
                ),
                "insert_into with duplicate primary key must fail",
            )
        )
        .and_then(
            lambda _: expect_equal(
                db.sales.insert_or_ignore(
                    DataFrames.CONFLICTING_SALES,
                )
                .read()
                .shape,
                (4, 3),
                "insert_or_ignore result shape",
            )
        )
        .and_then(
            lambda _: expect_equal(
                db.sales.read().filter(Sales.order_id.pl_col.eq(2)).item(0, "amount"),
                20.0,
                "insert_or_ignore keeps existing amount for order_id=2",
            ),
        )
        .and_then(
            lambda _: expect_equal(
                db.sales.insert_or_replace(DataFrames.CONFLICTING_SALES)
                .scan()
                .filter(Sales.order_id.nw_col == order_id)
                .collect()
                .item(0, "amount"),
                99.9,
                f"insert_or_replace overrides amount for order_id={order_id}",
            )
        )
        .and_then(
            lambda _: expect_raises(
                ConstraintException,
                lambda: db.sales.insert_into(DataFrames.UNIQUE_CONFLICT_SALES),
                "insert_into with UNIQUE conflict must fail",
            )
        )
        .and_then(
            lambda _: expect_equal(
                db.sales.truncate().read().shape, (0, 3), "truncate sales table"
            )
        )
        .and_then(
            lambda _: expect_raises(
                CatalogException,
                lambda: db.sales.drop().scan(),
                "drop non-existing table must raise CatalogException",
            )
        )
        .and_then(
            lambda _: expect_equal(
                TestData.sales_file.read_cast().shape,
                (3, 3),
                "test_file_operations: sales_file.read_cast",
            )
        )
    )


def run_tests() -> None:
    print("🚀 Démarrage des tests de framelib...")

    result: TestResult = pc.Err("no result")
    try:
        result = (
            from_callable(
                setup_folder,
                lambda e: f"setup_folder failed: {e}",
                OSError,
            )
            .map(lambda _: None)
            .and_then(lambda _: TestData.db.apply(setup_test_data).pipe(test_db_op))
        )
    except Exception as e:
        result = pc.Err(f"unhandled exception during tests: {e}")
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
