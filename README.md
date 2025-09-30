# framelib: pathlib declarative schemas for polars users

This package provides schema and classes to work with file system paths using `pathlib`.
It allows you to define directory structures and the files within them in a declarative manner.
You can then read and scan these files using the `polars` engine, as well as visualize the directory structure in a tree format.

This allows you to:

- Organize your data files in a structured way
- Self-document your data structure
- Easily read and manipulate data files using `polars`
- Visualize the directory structure for better understanding and navigation
- Implement conjointly with dataframely to combine both dataframe schemas and file system schemas.

## Installation

```bash
uv add git+https://github.com/OutSquareCapital/framelib.git
```

## Features

- define directory structures and files within them
- read/scan them using polars engine
- visualize them in a tree format.
- supports CSV, NDJson, JSON, Parquet and partitionned Parquet file formats

```python
import framelib as fl

class MyData(fl.Folder):
    sales = fl.CSV()
    customers = fl.NDJson()


class MySubFolder(MyData):
    reports = fl.Parquet()
    big_data = fl.Parquet(glob=True)


print(MyData.sales.path)  # mydata\sales.csv
print(MySubFolder.sales.path)  # mydata\sales.csv
print(MySubFolder.reports.path)  # mydata\mysubfolder\reports.parquet
print(MySubFolder.big_data.path)  # mydata\mysubfolder\big_data
MyData.sales.read()  # reads the CSV file as a Polars DataFrame
MyData.sales.scan(any_argument=...)  # scans the CSV file as a Polars LazyFrame
```

## Example

Below is a complete example of:

- defining dataframely schemas for the dataframes
- defining a folder structure linking the schemas to their positions
- functions for creating mock data files, and reading them using `framelib`.

This file can be found in `tests/examples.py`.

```python
import dataframely as dy
import polars as pl

import framelib as fl


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


# This will generate a __directory__ set to Path("tests")
class Tests(fl.Folder):
    pass


# By inheriting from Tests, the __directory__ will be Path("tests").joinpath("data")
class Data(Tests):
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


if __name__ == "__main__":
    mock_sales(Data.sales)
    mock_customers(Data.customers)
    mock_partitioned_parquet(Data.data_glob)
    assert Data.sales.path.as_posix() == "tests/data/sales.csv"
    assert Data.customers.path.as_posix() == "tests/data/customers.ndjson"
    assert Data.sales.read().shape == (3, 3)
    assert Data.data_glob.read().shape == (30, 6)
```

### Tree and schema Visualization

Easily visualize the existing files and folders in a tree format.
Note that this only shows **existing** files and folders.

You can also easily visualize the underlying dataframely schema for each file.

```python
print(Data.show_tree())
print(Data.data_glob.schema)
```

This will output something like:

```bash
tests\data
├── data_glob
│   ├── customer_id=101
│   │   └── 0.parquet
│   ├── customer_id=102
│   │   └── 0.parquet
│   ├── customer_id=103
│   │   └── 0.parquet
│   ├── customer_id=104
│   │   └── 0.parquet
│   └── customer_id=105
│       └── 0.parquet
├── customers.ndjson
└── sales.csv
[Schema "PartitionedSales"]
  Columns:
    - "order_id": UInt64(nullable=False, primary_key=True)
    - "customer_id": UInt64(nullable=False)
    - "amount": Float64(nullable=False)
    - "order_date": Date(nullable=False)
    - "region": String(nullable=False)
    - "product": String(nullable=False)

```
