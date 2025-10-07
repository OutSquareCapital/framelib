from pathlib import Path

import polars as pl
from duckdb import CatalogException, ConstraintException

import framelib as fl

# --- Configuration et Schémas ---

BASE_PATH = Path("tests")


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
    __source__ = BASE_PATH
    sales_file = fl.CSV(model=Sales)
    customers_file = fl.NDJson(model=Customers)
    db = TestDB()


# --- Données de Test ---

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


def setup_test_data() -> None:
    """Crée les fichiers et la base de données de test."""
    TestData.source().mkdir(parents=True, exist_ok=True)
    TestData.sales_file.write(SALES_DATA)
    with TestData.db as db:
        db.sales.create_or_replace_from(SALES_DATA)


def teardown_test_data() -> None:
    """Nettoie les données de test."""
    TestData.clean()


def run_tests() -> None:
    """Exécute tous les tests."""
    print("🚀 Démarrage des tests de framelib...")

    try:
        setup_test_data()
        print("\n--- ✅ Tests de la Base de Données ---")
        test_database_operations()

        print("\n--- ✅ Tests des Fichiers ---")
        test_file_operations()

        print("\n🎉 Tous les tests sont passés avec succès!")

    except Exception as e:
        print(f"❌ ERREUR PENDANT LES TESTS: \n{e}")
    finally:
        teardown_test_data()
        print("\n🧹 Nettoyage terminé.")


def test_file_operations() -> None:
    """Teste la lecture et l'écriture de fichiers."""
    print("▶️ Test: CSV read_cast...")
    assert TestData.sales_file.read_cast().shape == (3, 3)
    print("✅ OK")


def test_database_operations() -> None:
    with TestData.db as db:
        print("▶️ Test: create_or_replace_from & scan...")
        assert db.sales.scan().collect().shape == (3, 3)
        print("✅ OK")

        print(db.sales.describe_columns())

        print("\n▶️ Test: insert_into (conflit PK)...")
        try:
            db.sales.insert_into(CONFLICTING_SALES.filter(Sales.order_id.pl_col.eq(2)))
            assert False, "ConstraintException non levée pour insert_into."
        except ConstraintException:
            print("✅ OK (erreur attendue capturée)")
        print("\n▶️ Test: insert_or_ignore (conflit PK)...")
        result: pl.DataFrame = db.sales.insert_or_ignore(CONFLICTING_SALES).read()
        assert result.shape == (4, 3)
        assert result.filter(Sales.order_id.pl_col.eq(2)).item(0, "amount") == 20.0
        print("✅ OK")

        print("\n▶️ Test: insert_or_replace (conflit PK)...")
        updated_amount = (
            db.sales.insert_or_replace(CONFLICTING_SALES)
            .scan()
            .collect()
            .filter(Sales.order_id.nw_col == 2)
            .item(0, "amount")
        )
        assert updated_amount == 99.9
        print("✅ OK")

        print("\n▶️ Test: contrainte UNIQUE...")
        try:
            db.sales.insert_into(UNIQUE_CONFLICT_SALES)
        except ConstraintException:
            print("✅ OK (erreur attendue capturée)")

        # 6. Test de `truncate` et `drop`
        print("\n▶️ Test: truncate & drop...")
        assert db.sales.truncate().read().shape == (0, 3)
        try:
            db.sales.drop().scan()
        except CatalogException:
            print("✅ OK")


if __name__ == "__main__":
    run_tests()
