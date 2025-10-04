# Framelib: Declarative Data Architecture

Framelib transforms how you manage data projects.

Instead of juggling hardcoded paths and implicit data structures, you can define your entire data architecture—files, folders, schemas, and even embedded databases—as clean, self-documenting, and type-safe Python classes.

It leverages **pathlib**, **polars**, **narwhals**, and **duckdb** to provide a robust framework for building maintainable and scalable data pipelines.

## Why Framelib?

### 🏛️ Declare Your Architecture Once

Define your project's file and database layout using intuitive Python classes.

Each class represents a folder, file, types schema, or database table, making your data structure explicit and easy to understand.

If no **source** is provided, the source path is automatically inferred from the class name and its position in the hierarchy.

This applies for each file declared as an attribute of a Folder class, and each Column declared in a Schema class.

Define once, use everywhere. Your data structure definitions are reusable across your entire codebase.

### 📜 Enforce Data Contracts

Framelib provides a **Schema** class, with an API strongly inspired by dataframely, to define data schemas with strong typing and validation.

A **Schema** is a specialized **Layout** that only accepts **Column** entries.

A **Column** represents a single column in a data schema, with optional primary key designation.

Various **Column** types are available, such as **Int32**, **Enum**, **Struct**, and more.

Each **Column** can then be converted to it's corresponding polars, narwhals, or SQL datatype.

For example **Column.UInt32.pl_dtype** returns an instance of **pl.UInt32**.

You can cast data to the defined schema when reading from files or databases, ensuring consistency and reducing runtime errors.

This interoperability and data validation maintains the core declarative DRY philosophy of framelib.

### 🚀 Streamline Workflows

Read, write, and process data with a high-level API that abstracts away boilerplate code.

You don't have to manually pass your argument to polars.scan_parquet ever again. simply call `MyFolder.myfile.scan()` and framelib handles the rest.

At a glance, you can then check:

- where is my data stored?
- in which format?
- with which schema?

### 🌲 Visualize Your Project

Automatically generate a recursive tree view of your data layout for easy navigation and documentation.

### 📦 Embedded Data Warehouse

Manage and query an embedded DuckDB database with the same declarative approach.

Get back your DuckDB queries as narwhals lazyframe, and write your queries with the polars syntax.

## Installation

```bash
uv add git+https://github.com/OutSquareCapital/framelib.git
```

## Quickstart

### Declare Your Data Architecture

```python
import duckdb
import narwhals as nw
import polars as pl

import framelib as fl


class Sales(fl.Schema):
    """Schema for the sales."""

    transaction_id = fl.UInt32(primary_key=True)
    customer_id = fl.UInt16()
    amount = fl.Float32()


class Analytics(fl.DataBase):
    """Embedded DuckDB database for analytics. Contain a sales table."""

    sales = fl.Table(Sales)


class MyProject(fl.Folder):
    """Root folder for the project. __source__ automatically set to Path("myproject")"""

    ## Files are defined as attributes
    raw_sales = fl.CSV(model=Sales)  # Located at 'myproject/raw_sales.csv'

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
        print("\n📦 Sales Data in DB after append:")
        print(db.sales.append(new_sales).scan().to_native())
        ## Intelligently insert rows, skipping duplicates based on the primary key
        print("\n📦 Sales Data in DB after insert_if_not_exists (no duplicates):")
        print(db.sales.insert_if_not_exists(new_sales).scan().to_native())
        print("\n📦 Sales Data in DB after truncate:")
        print(db.sales.truncate().scan().to_native())
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

def show_inheritance_and_tree() -> None:
    class ProductionData(fl.Folder):
        sales = fl.CSV(model=Sales)
    class Reports(ProductionData):
        """
        Reports folder inheriting from ProductionData.
        No schema defined in the files, so they default to framelib.Schema.
        """
        sales = fl.CSV() 
        sales_formatted = fl.Parquet()


    print("\n📁 Inheritance Example:\n")
    print(ProductionData.sales.source)
    print(Reports.sales.source)
    print(Reports.sales_formatted.source)
    print("\n📂 Project Structure:\n")
    print(Reports.show_tree())

```

```bash
📂 Project Structure:

productiondata
├── reports
│   ├── sales.csv
│   └── sales_formatted.parquet
└── sales.csv

```

### Cast data to the defined schema

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
