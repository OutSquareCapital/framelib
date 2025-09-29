import polars as pl

import framelib as fl


# This will generate a __directory__ set to Path("tests")
class Tests(fl.Folder):
    pass


# By inheriting from Tests, the __directory__ will be Path("tests").joinpath("data")
class Data(Tests):
    sales = fl.CSV()  # "tests/data/sales.csv"
    customers = fl.NDJson()  # "tests/data/customers.ndjson"
    data_glob = fl.Parquet(glob=True)  # "tests/data/data_glob"


def mock_sales(reader: fl.CSV) -> None:
    pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "amount": [250.0, 450.5, 300.75],
        }
    ).write_csv(reader.path)


def mock_customers(reader: fl.NDJson) -> None:
    pl.DataFrame(
        {
            "customer_id": [101, 102, 103],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        }
    ).write_ndjson(reader.path)


def mock_partitioned_parquet(reader: fl.Parquet) -> None:
    pl.DataFrame(
        {
            "order_id": list(range(1, 31)),
            "customer_id": [101, 102, 103, 104, 105] * 6,
            "amount": [float(x) for x in range(100, 130)],
            "order_date": [f"2024-01-{i:02d}" for i in range(1, 31)],
            "region": ["north", "south", "east", "west", "central"] * 6,
            "product": ["A", "B", "C", "D", "E"] * 6,
        }
    ).write_parquet(reader.path, partition_by="customer_id")


if __name__ == "__main__":
    mock_sales(Data.sales)
    mock_customers(Data.customers)
    mock_partitioned_parquet(Data.data_glob)
    assert Data.sales.path.as_posix() == "tests/data/sales.csv"
    assert Data.customers.path.as_posix() == "tests/data/customers.ndjson"
    assert Data.sales.read().shape == (3, 3)
    assert Data.data_glob.read().shape == (30, 6)
    print(Data.show_tree())
