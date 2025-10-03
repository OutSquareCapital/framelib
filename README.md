# Framelib: Declarative Data Architecture

Framelib transforms how you manage data projects.

Instead of juggling hardcoded paths and implicit data structures, you can define your entire data architecture—files, folders, schemas, and even embedded databases—as clean, self-documenting, and type-safe Python classes.

It leverages **pathlib**, **polars**, **narwhals**, and **duckdb** to provide a robust framework for building maintainable and scalable data pipelines.

## Why Framelib?

🏛️ **Declare Your Architecture:** Define your project's file and database layout using intuitive Python classes.

📜 **Enforce Data Contracts:** Integrate with dataframely to ensure data quality with schema-aware I/O operations.

🚀 **Streamline Workflows:** Read, write, and process data with a high-level API that abstracts away boilerplate code.

🌲 **Visualize Your Project:** Automatically generate a tree view of your data layout for easy navigation and documentation.

📦 **Embedded Data Warehouse:** Manage and query an embedded DuckDB database with the same declarative approach.

📃 **Write SQL in polars syntax:** Get back your DuckDB queries as narwhals lazyframe, and write your queries with the polars syntax.

## Installation

```bash
uv add git+https://github.com/OutSquareCapital/framelib.git
```

## Quickstart

### Declare Your Data Architecture

```python
import dataframely as dy
import duckdb
import narwhals as nw
import polars as pl

import framelib as fl


class SalesFile(dy.Schema):
    """Schema for the raw sales CSV file."""

    transaction_id = dy.UInt32(nullable=False)
    customer_id = dy.UInt16(nullable=False)
    amount = dy.Float32(nullable=False)


class SalesDB(fl.Schema):
    """Schema for the sales table in the database."""

    transaction_id = fl.UInt32(primary_key=True)
    customer_id = fl.UInt16()
    amount = fl.Float32()


class Analytics(fl.DataBase):
    """Embedded DuckDB database for analytics. Contain a sales table."""

    sales = fl.Table(SalesDB)


class MyProject(fl.Folder):
    """Root folder for the project. __source__ automatically set to Path("myproject")"""

    ## Files are defined as attributes
    raw_sales = fl.CSV(model=SalesFile)  # Located at 'myproject/raw_sales.csv'

    ## Instantiate the embedded database
    analytics_db = Analytics()  # Located at 'myproject/analytics_db.ddb'
```

### Create the structure on disk

```python

def create_structure() -> None:
    MyProject.source().mkdir(parents=True, exist_ok=True)
    print(f"✅ Project structure created at: {MyProject.source().as_posix()}")
```

```bash
✅ Project structure created at: myproject
```

### Create mock sales data

Write data to the CSV, automatically passing the path argument.

Since write/read/scan properties returns partials, pass any native polars argument with IDE support for documentation and argument validity.

```python

def create_mock_sales_data() -> None:
    mock_sales_data = pl.DataFrame(
        {
            "transaction_id": [101, 102, 103],
            "customer_id": [1, 2, 1],
            "amount": [120.50, 75.00, 50.25],
        }
    )
    MyProject.raw_sales.write(mock_sales_data, retries=2)
    print(f"✅ Raw sales data written to: {MyProject.raw_sales.source}")
```

```bash
✅ Raw sales data written to: myproject\raw_sales.csv
```

### Load data into the DuckDB database and generate a report

```python
def load_data_into_db() -> None:
    raw_df: pl.LazyFrame = MyProject.raw_sales.scan_cast()

    with MyProject.analytics_db as db:
        db.sales.create_or_replace_from(raw_df)
        print("✅ Data loaded into DuckDB.")

        ## Query the data directly from the database using the Narwhals API
        report_df: duckdb.DuckDBPyRelation = (
            db.sales.scan()
            .group_by("customer_id")
            .agg(
                total_spent=nw.col("amount").sum(),
                transaction_count=nw.len(),
            )
            .to_native()
        )
        print("\n📊 Generated Report:")
        print(report_df)
```

```bash
✅ Data loaded into DuckDB.

📊 Generated Report:
┌─────────────┬─────────────┬───────────────────┐
│ customer_id │ total_spent │ transaction_count │
│   uint16    │   double    │       int64       │
├─────────────┼─────────────┼───────────────────┤
│           1 │      170.75 │                 2 │
│           2 │        75.0 │                 1 │
└─────────────┴─────────────┴───────────────────┘
```

### Inerhit for nested structures

```python

def show_inheritance_example() -> None:
    class ProductionData(fl.Folder):
        sales = fl.CSV(model=SalesFile)

    class Reports(ProductionData):
        sales = fl.CSV(dy.Schema)
        sales_formatted = fl.Parquet(dy.Schema)

    print("\n📁 Inheritance Example:")
    print(Reports.sales.source)
    print(Reports.sales_formatted.source)
```

```bash
📁 Inheritance Example:
productiondata\reports\sales.csv
productiondata\reports\sales_formatted.parquet
```

### Read and cast data

```python
def read_and_cast() -> None:
    print("\n📋 Raw Sales Data:")
    print(MyProject.raw_sales.read().schema)
    print("Casted to the defined schema:")
    print(MyProject.raw_sales.read_cast().schema)
```

```bash
📋 Raw Sales Data:
Schema({'transaction_id': Int64, 'customer_id': Int64, 'amount': Float64})
Casted to the defined schema:
Schema({'transaction_id': UInt32, 'customer_id': UInt16, 'amount': Float32})
```

### Append data and perform various database operations

```python

def append_data() -> None:
    new_sales = pl.DataFrame(
        {
            "transaction_id": [104, 105],
            "customer_id": [3, 2],
            "amount": [200.00, 150.75],
        }
    )

    with MyProject.analytics_db as db:
        ## High-level methods simplify common database operations
        print("\n📦 Sales Data in DB before append:")
        print(db.sales.scan().to_native())
        db.sales.append(new_sales)
        print("\n📦 Sales Data in DB after append:")
        print(db.sales.scan().to_native())
        ## Intelligently insert rows, skipping duplicates based on the primary key
        db.sales.insert_if_not_exists(new_sales)
        print("\n📦 Sales Data in DB after insert_if_not_exists (no duplicates):")
        print(db.sales.scan().to_native())
        db.sales.truncate()
        print("\n📦 Sales Data in DB after truncate:")
        print(db.sales.scan().to_native())
```

```bash

📦 Sales Data in DB before append:
┌────────────────┬─────────────┬────────┐
│ transaction_id │ customer_id │ amount │
│     uint32     │   uint16    │ float  │
├────────────────┼─────────────┼────────┤
│            101 │           1 │  120.5 │
│            102 │           2 │   75.0 │
│            103 │           1 │  50.25 │
└────────────────┴─────────────┴────────┘


📦 Sales Data in DB after append:
┌────────────────┬─────────────┬────────┐
│ transaction_id │ customer_id │ amount │
│     uint32     │   uint16    │ float  │
├────────────────┼─────────────┼────────┤
│            101 │           1 │  120.5 │
│            102 │           2 │   75.0 │
│            103 │           1 │  50.25 │
│            104 │           3 │  200.0 │
│            105 │           2 │ 150.75 │
└────────────────┴─────────────┴────────┘


📦 Sales Data in DB after insert_if_not_exists (no duplicates):
┌────────────────┬─────────────┬────────┐
│ transaction_id │ customer_id │ amount │
│     uint32     │   uint16    │ float  │
├────────────────┼─────────────┼────────┤
│            101 │           1 │  120.5 │
│            102 │           2 │   75.0 │
│            103 │           1 │  50.25 │
│            104 │           3 │  200.0 │
│            105 │           2 │ 150.75 │
└────────────────┴─────────────┴────────┘


📦 Sales Data in DB after truncate:
┌────────────────┬─────────────┬────────┐
│ transaction_id │ customer_id │ amount │
│     uint32     │   uint16    │ float  │
├────────────────┴─────────────┴────────┤
│                0 rows                 │
└───────────────────────────────────────┘
```

### Show the project structure

```python
def show_tree() -> None:
    print("\n📂 Project Structure:")
    print(MyProject.show_tree())
```

```bash
📂 Project Structure:
myproject
├── analytics_db.ddb
└── raw_sales.csv
```

### Clean up the project structure

```python
def clean_project() -> None:
    MyProject.clean()
    print("\n✅ Project structure cleaned up.")
    try:
        MyProject.raw_sales.read()
    except FileNotFoundError:
        print("✅ Confirmed: Raw sales file no longer exists.")
```

```bash
✅ Project structure cleaned up.
✅ Confirmed: Raw sales file no longer exists.
```

## Credits

Heavily inspired by dataframely: <https://github.com/quantco/dataframely>

## License

MIT License. See [LICENSE](./LICENSE) for details.
