from pathlib import Path

import dataframely as dy
import polars as pl

import framelib as fl

BASE_PATH = Path("tests")


class SalesDB(fl.Schema):
    order_id = fl.UInt16()
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


class Data(fl.Folder):
    __source__ = BASE_PATH
    sales = fl.CSV(model=Sales)  # "tests/data/sales.csv"
    customers = fl.NDJson(model=Customers)  # "tests/data/customers.ndjson"
    data_glob = fl.ParquetPartitioned(
        "customer_id",
        PartitionedSales,  # "tests/data/data_glob"
    )
    dataduck = Duck()


print("helo")
print(Data.schema())
print(Data.dataduck.schema())
print("fdp")


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


if __name__ == "__main__":
    print(Data.source())
    mock_sales(Data.sales)
    mock_customers(Data.customers)
    mock_partitioned_parquet(Data.data_glob)
    mock_tables()
    assert Data.sales.source.as_posix() == "tests/data/sales.csv"
    assert Data.customers.source.as_posix() == "tests/data/customers.ndjson"
    assert Data.sales.read().shape == (3, 3)
    assert Data.data_glob.read().shape == (30, 6)
    try:
        Data.dataduck.salesdb.read()
    except Exception as e:
        print(f"Error reading salesdb: {e}")
    with Data.dataduck as db:
        print(db.salesdb.read().collect())
