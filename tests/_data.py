import polars as pl

import framelib as fl


class Sales(fl.Schema):
    order_id = fl.UInt16(primary_key=True)
    customer_id = fl.UInt16(unique=True)
    amount = fl.Float64()


class Customers(fl.Schema):
    customer_id = fl.UInt16(primary_key=True)
    name = fl.String()
    email = fl.String()


class SampleDB(fl.DataBase):
    sales = fl.Table(Sales)
    customers = fl.Table(Customers)


class TestData(fl.Folder):
    sales_file = fl.CSV(model=Sales)
    customers_file = fl.NDJson(model=Customers)
    db = SampleDB()


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
        schema=Customers.to_pl(),
    )

    SALES = pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "amount": [10.0, 20.0, 30.0],
        },
        schema=Sales.to_pl(),
    )
    CONFLICTING_SALES = pl.DataFrame(
        {"order_id": [2, 4], "customer_id": [102, 104], "amount": [99.9, 40.0]},
    )
    UNIQUE_CONFLICT_SALES = pl.DataFrame(
        {"order_id": [5], "customer_id": [101], "amount": [50.0]},
    )
