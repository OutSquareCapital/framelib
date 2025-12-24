from pathlib import Path

import polars as pl
import pyochain as pc

import framelib as fl


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


class DataFrames:
    CUSTOMERS = pl.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
            ],
        },
    )

    SALES = pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "amount": [10.0, 20.0, 30.0],
        },
    )
    CONFLICTING_SALES = pl.DataFrame(
        {"order_id": [2, 4], "customer_id": [102, 104], "amount": [99.9, 40.0]},
    )
    UNIQUE_CONFLICT_SALES = pl.DataFrame(
        {"order_id": [5], "customer_id": [101], "amount": [50.0]},
    )


def setup_folder() -> pc.Result[None, OSError]:
    try:
        TestData.source().mkdir(parents=True, exist_ok=True)
        TestData.sales_file.write(DataFrames.SALES)
        print(TestData.show_tree())
        return pc.Ok(None)
    except Exception as e:
        msg = f"setup_folder failed: {e}"
        return pc.Err(OSError(msg))
