from pathlib import Path

import dataframely as dy
import polars as pl
from duckdb import DuckDBPyRelation

import framelib as fl

BASE_PATH = Path("tests")


class Duck(fl.duck.DataBase):
    __directory__ = BASE_PATH
    salesdb = fl.duck.Table()
    customersdb = fl.duck.Table()


class Sales(dy.Schema):
    order_id = dy.UInt64(primary_key=True, nullable=False)
    customer_id = dy.UInt64(nullable=False)
    amount = dy.Float64(nullable=False)


class Customers(dy.Schema):
    customer_id = dy.UInt64(primary_key=True, nullable=False)
    name = dy.String(nullable=False)
    email = dy.String(nullable=False)


class PartitionedSales(dy.Schema):
    order_id = dy.UInt64(primary_key=True, nullable=False)
    customer_id = dy.UInt64(nullable=False)
    amount = dy.Float64(nullable=False)
    order_date = dy.Date(nullable=False)
    region = dy.String(nullable=False)
    product = dy.String(nullable=False)


# By inheriting from Tests, the __directory__ will be Path("tests").joinpath("data")
class Data(fl.Folder):
    __directory__ = BASE_PATH
    sales = fl.CSV(schema=Sales)  # "tests/data/sales.csv"
    customers = fl.NDJson(schema=Customers)  # "tests/data/customers.ndjson"
    data_glob = fl.ParquetPartitioned(
        "customer_id",
        PartitionedSales,  # "tests/data/data_glob"
    )


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
    with Duck() as db:
        db.salesdb.write(sales)
        db.customersdb.write(customers)


def test_nw() -> None:
    import duckdb
    import polars as pl

    class Foo(fl.duck.Schema):
        a = fl.duck.Float32()
        b = fl.duck.Float32()
        c = fl.duck.String()

    df = pl.DataFrame({"a": [1, 2], "b": [3.0, 4.0], "c": ["x", "y"]})

    df_duck: duckdb.DuckDBPyRelation = duckdb.sql("""SELECT * FROM df""")

    df_casted: pl.DataFrame = Foo.cast(df.lazy()).collect()
    duck_casted: DuckDBPyRelation = Foo.cast(df_duck).execute()
    print("polars casted:")
    print(df_casted)
    print("duckdb casted:")
    print(duck_casted)


if __name__ == "__main__":
    mock_sales(Data.sales)
    mock_customers(Data.customers)
    mock_partitioned_parquet(Data.data_glob)
    assert Data.sales.path.as_posix() == "tests/data/sales.csv"
    assert Data.customers.path.as_posix() == "tests/data/customers.ndjson"
    assert Data.sales.read().shape == (3, 3)
    assert Data.data_glob.read().shape == (30, 6)
    print(Data.show_tree())
    print(Data.data_glob.schema)
    mock_tables()
    with Duck() as db:
        print(db.salesdb.read().schema)
        print(db.salesdb.read().to_native())
        print(db.customersdb.read().to_native())
    test_nw()
