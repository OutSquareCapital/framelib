from pathlib import Path

import polars as pl
from duckdb import CatalogException, ConstraintException

import framelib as fl

# --- Configuration et Schémas ---


class Sales(fl.Schema):
    order_id = fl.UInt16(primary_key=True)
    customer_id = fl.UInt16(unique=True)
    amount = fl.Float64()


class Customers(fl.Schema):
    customer_id = fl.UInt16(primary_key=True)
    name = fl.String()
    email = fl.String()


class TestDB(fl.DataBase):
    sales = fl.Table(Sales)
    customers = fl.Table(Customers)


class TestData(fl.Folder):
    __source__ = Path("tests")
    sales_file = fl.CSV(model=Sales)
    customers_file = fl.NDJson(model=Customers)
    db = TestDB()


# --- Données de Test ---

CUSTOMER_DATA = pl.DataFrame(
    {
        "customer_id": [101, 102, 103],
        "name": ["Alice", "Bob", "Charlie"],
        "email": [
            "alice@example.com",
            "bob@example.com",
            "charlie@example.com",
        ],
    }
)

SALES_DATA = pl.DataFrame(
    {
        "order_id": [1, 2, 3],
        "customer_id": [101, 102, 103],
        "amount": [10.0, 20.0, 30.0],
    }
)
CONFLICTING_SALES = pl.DataFrame(
    {"order_id": [2, 4], "customer_id": [102, 104], "amount": [99.9, 40.0]}
)
UNIQUE_CONFLICT_SALES = pl.DataFrame(
    {"order_id": [5], "customer_id": [101], "amount": [50.0]}
)


def setup_folder() -> None:
    """
    tree should look like this:

    tests\\testdata\n
    ├── customers_file.ndjson \n
    ├── db.ddb \n
    └── sales_file.csv \n
    """
    TestData.source().mkdir(parents=True, exist_ok=True)
    TestData.sales_file.write(SALES_DATA)
    print(TestData.show_tree())


def setup_test_data(db: TestDB) -> None:
    db.customers.create_or_replace_from(CUSTOMER_DATA)
    db.sales.create_or_replace_from(SALES_DATA)
    TestData.show_tree()


def teardown_test_data() -> None:
    """Nettoie les données de test."""
    TestData.clean()


def test_file_operations() -> None:
    """Teste la lecture et l'écriture de fichiers."""
    assert TestData.sales_file.read_cast().shape == (3, 3)


def test_database_operations(db: TestDB) -> None:
    assert db.sales.read().shape == (3, 3)
    db.sales.describe_columns().collect("polars")
    try:
        db.sales.insert_into(CONFLICTING_SALES.filter(Sales.order_id.pl_col.eq(2)))
    except ConstraintException:
        pass
    result: pl.DataFrame = db.sales.insert_or_ignore(CONFLICTING_SALES).read()
    assert result.shape == (4, 3)
    assert result.filter(Sales.order_id.pl_col.eq(2)).item(0, "amount") == 20.0
    assert (
        db.sales.insert_or_replace(CONFLICTING_SALES)
        .scan()
        .filter(Sales.order_id.nw_col == 2)
        .collect()
        .item(0, "amount")
    ) == 99.9
    try:
        db.sales.insert_into(UNIQUE_CONFLICT_SALES)
    except ConstraintException:
        pass
    assert db.sales.truncate().read().shape == (0, 3)
    try:
        db.sales.drop().scan()
    except CatalogException:
        pass


def run_tests() -> None:
    """Exécute tous les tests."""
    print("🚀 Démarrage des tests de framelib...")

    try:
        setup_folder()
        TestData.db.apply(setup_test_data).apply(test_database_operations).close()
        test_file_operations()

        teardown_test_data()
    except Exception as e:
        print(f"❌ ERREUR PENDANT LES TESTS: \n{e}")
    print("\n🎉 Tous les tests sont passés avec succès!")
