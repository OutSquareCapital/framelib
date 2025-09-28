# framelib: pathlib declarative schemas

This package provides schema and classes to work with file system paths using `pathlib`.

It includes features for:

- defining directory structures
- declaring the files in it
- reading/scanning them using polars engine
- visualizing them in a tree format.

## Example

```python
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
    """Output:
    tests\data
    ├── customers.ndjson
    └── sales.csv
    """
    print(MyDirectory.sales.read())
    """Output:
    shape: (3, 3)
    ┌──────────┬─────────────┬────────┐
    │ order_id ┆ customer_id ┆ amount │
    │ ---      ┆ ---         ┆ ---    │
    │ i64      ┆ i64         ┆ f64    │
    ╞══════════╪═════════════╪════════╡
    │ 1        ┆ 101         ┆ 250.0  │
    │ 2        ┆ 102         ┆ 450.5  │
    │ 3        ┆ 103         ┆ 300.75 │
    └──────────┴─────────────┴────────┘
    """
    print(MyDirectory.customers.read())
    """
    Output:
    shape: (3, 3)
    ┌─────────────┬─────────┬─────────────────────┐
    │ customer_id ┆ name    ┆ email               │
    │ ---         ┆ ---     ┆ ---                 │
    │ i64         ┆ str     ┆ str                 │
    ╞═════════════╪═════════╪═════════════════════╡
    │ 101         ┆ Alice   ┆ alice@example.com   │
    │ 102         ┆ Bob     ┆ bob@example.com     │
    │ 103         ┆ Charlie ┆ charlie@example.com │
    └─────────────┴─────────┴─────────────────────┘
    """

```

## Installation

```bash
uv add git+https://github.com/OutSquareCapital/framelib.git
```
