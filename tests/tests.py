from pathlib import Path

import dataframely as dy
import duckdb
import polars as pl
from duckdb import CatalogException

import framelib as fl

BASE_PATH = Path("tests")


class SalesDB(fl.Schema):
    order_id = fl.UInt16(primary_key=True)
    customer_id = fl.UInt16()
    amount = fl.Float32()


class CustomersDB(fl.Schema):
    customer_id = fl.UInt16()
    name = fl.String()
    email = fl.String()


class Duck(fl.DataBase):
    salesdb = fl.Table(SalesDB)
    customersdb = fl.Table(CustomersDB)


class Sales(dy.Schema):
    order_id = dy.UInt16(primary_key=True, nullable=False)
    customer_id = dy.UInt16(nullable=False)
    amount = dy.Float64(nullable=False)


class Customers(dy.Schema):
    customer_id = dy.UInt16(primary_key=True, nullable=False)
    name = dy.String(nullable=False)
    email = dy.String(nullable=False)


class PartitionedSales(dy.Schema):
    order_id = dy.UInt16(primary_key=True, nullable=False)
    customer_id = dy.UInt16(nullable=False)
    amount = dy.Float64(nullable=False)
    order_date = dy.Date(nullable=False)
    region = dy.String(nullable=False)
    product = dy.String(nullable=False)


class Data(fl.Folder):
    __source__ = BASE_PATH
    sales = fl.CSV(model=Sales)
    customers = fl.NDJson(model=Customers)
    data_glob = fl.ParquetPartitioned(
        "customer_id",
        PartitionedSales,
    )
    dataduck = Duck()


def mock_sales(file: fl.CSV[Sales]) -> None:
    pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "amount": [250.0, 450.5, 300.75],
        }
    ).pipe(file.write)


def mock_customers(file: fl.NDJson[Customers]) -> None:
    pl.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        }
    ).pipe(file.write)


def mock_partitioned_parquet(file: fl.Parquet[PartitionedSales]) -> None:
    pl.DataFrame(
        {
            "order_id": list(range(1, 31)),
            "customer_id": [101, 102, 103, 104, 105] * 6,
            "amount": [float(x) for x in range(100, 130)],
            "order_date": [f"2024-01-{i:02d}" for i in range(1, 31)],
            "region": ["north", "south", "east", "west", "central"] * 6,
            "product": ["A", "B", "C", "D", "E"] * 6,
        }
    ).pipe(file.write)


def mock_tables() -> None:
    sales = pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "amount": [250.0, 450.5, 300.75],
        }
    )

    customers = pl.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        }
    )
    with Data.dataduck as db:
        db.salesdb.create_or_replace_from(sales)
        db.customersdb.create_or_replace_from(customers)

    print("ok")


def run_file_tests() -> None:
    print("\n--- DÉBUT DU TEST DES FICHIERS ---")

    # Test CSV
    print("▶️ Test: CSV read and write...")
    csv_file = Data.sales
    df_csv = csv_file.read_cast()
    print(df_csv)
    assert df_csv.shape == (3, 3)
    print("✅ OK")

    # Test NDJson
    print("\n▶️ Test: NDJson read and write...")
    ndjson_file = Data.customers
    df_ndjson = ndjson_file.read_cast()
    print(df_ndjson)
    assert df_ndjson.shape == (3, 3)
    print("✅ OK")

    # Test ParquetPartitioned
    print("\n▶️ Test: ParquetPartitioned read and write...")
    parquet_file = Data.data_glob
    df_parquet = parquet_file.scan_cast().collect()
    print(df_parquet)
    assert df_parquet.shape == (30, 6)
    print("✅ OK")

    print("\n--- FIN DU TEST DES FICHIERS ---")


def run_quick_table_tests() -> None:
    print("\n--- DÉBUT DU TEST RAPIDE DES TABLES ---")

    initial_sales = pl.DataFrame(
        {"order_id": [1, 2], "customer_id": [101, 102], "amount": [10.0, 20.0]}
    )
    conflicting_sales = pl.DataFrame(
        {"order_id": [2, 3], "customer_id": [102, 103], "amount": [99.9, 30.0]}
    )
    customer_data = pl.DataFrame(
        {"customer_id": [201], "name": ["David"], "email": ["david@test.com"]}
    )

    with Data.dataduck as db:
        try:
            print("▶️ Test: create_or_replace_from...")
            db.salesdb.create_or_replace_from(initial_sales)
            result = db.salesdb.scan().collect().to_native()
            print(result)
            assert result.shape == (2, 3)
            print("✅ OK")

            print("\n▶️ Test: append (with PK conflict)...")
            try:
                db.salesdb.append(conflicting_sales.filter(pl.col("order_id") == 2))
                raise AssertionError("ConstraintException was not raised for append.")
            except duckdb.ConstraintException as e:
                print(f"✅ OK (Caught expected error: {e})")

            print("\n▶️ Test: insert_if_not_exists (with PK conflict)...")
            db.salesdb.insert_if_not_exists(conflicting_sales)
            result = db.salesdb.scan().collect()
            print(result)

            assert result.shape == (3, 3)
            original_amount = result.filter(SalesDB.order_id.col == 2).item(0, "amount")
            assert original_amount == 20.0
            print("✅ OK")

            print("\n▶️ Test: insert_if_not_exists (on table without PK)...")
            try:
                db.customersdb.create_or_replace_from(customer_data)
                db.customersdb.insert_if_not_exists(customer_data)
                raise AssertionError("ValueError was not raised for table without PK.")
            except ValueError as e:
                print(f"✅ OK (Caught expected error: {e})")

            print("\n▶️ Test: truncate...")
            db.salesdb.truncate()
            result = db.salesdb.scan().to_native()
            print(result)
            assert result.shape == (0, 3)
            print("✅ OK")

            print("\n▶️ Test: drop...")
            db.salesdb.drop()
            try:
                db.salesdb.scan()
                raise AssertionError("Table was not dropped.")
            except CatalogException:
                print("✅ OK")

        except (Exception, AssertionError) as e:
            print(f"❌ ERREUR PENDANT LE TEST: {e}")
        finally:
            print("\n--- FIN DU TEST RAPIDE ---")


if __name__ == "__main__":
    mock_sales(Data.sales)
    mock_customers(Data.customers)
    mock_partitioned_parquet(Data.data_glob)
    mock_tables()
    run_file_tests()
    run_quick_table_tests()
