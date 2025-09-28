from pathlib import Path

import polars as pl

import framelib as fl


class MyDirectory(fl.Folder):
    __directory__ = Path("tests", "data")
    sales = fl.CSV()
    customers = fl.NDJson()


def mock_sales():
    pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "amount": [250.0, 450.5, 300.75],
        }
    ).write_csv(MyDirectory.sales.path)


def mock_customers():
    pl.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        }
    ).write_ndjson(MyDirectory.customers.path)


if __name__ == "__main__":
    mock_sales()
    mock_customers()
    assert MyDirectory.sales.path.as_posix() == "tests/data/sales.csv"
    assert MyDirectory.customers.path.as_posix() == "tests/data/customers.ndjson"
    print(MyDirectory.show_tree())
    print(MyDirectory.sales.read())
    print(MyDirectory.customers.read())
