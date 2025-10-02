# Framelib: Declarative Data Architecture

Framelib transforms how you manage data projects.

Instead of juggling hardcoded paths and implicit data structures, you can define your entire data architecture—files, folders, schemas, and even embedded databases—as clean, self-documenting, and type-safe Python classes.

It leverages pathlib, polars, and duckdb to provide a robust framework for building maintainable and scalable data pipelines.

## Why Framelib?

🏛️ **Declare Your Architecture:** Define your project's file and database layout using intuitive Python classes.

📜 **Enforce Data Contracts:** Integrate with dataframely to ensure data quality with schema-aware I/O operations.

🚀 **Streamline Workflows:** Read, write, and process data with a high-level API that abstracts away boilerplate code.

🌲 **Visualize Your Project:** Automatically generate a tree view of your data layout for easy navigation and documentation.

📦 **Embedded Data Warehouse:** Manage and query an embedded DuckDB database with the same declarative approach.

## Installation

```bash
uv add git+[https://github.com/OutSquareCapital/framelib.git](https://github.com/OutSquareCapital/framelib.git)
```

## Quickstart

Let's build a simple pipeline that reads raw data, loads it into a DuckDB database for analysis, and generates a report.

### Define Your Schemas

First, define the "data contracts" for your files and database tables.

Framelib uses dataframely for file schemas and has its own Schema object for database tables.

```python
from pathlib import Path
import dataframely as dy
import framelib as fl
import polars as pl
import narwhals as nw

## Schema for the raw input file (CSV)

class SalesFile(dy.Schema):
    transaction_id = dy.UInt32(nullable=False)
    customer_id = dy.UInt16(nullable=False)
    amount = dy.Float32(nullable=False)

## Schema for the database table

class SalesDB(fl.Schema):
    transaction_id = fl.UInt32(primary_key=True)
    customer_id = fl.UInt16()
    amount = fl.Float32()
```

### Declare Your Project Layout

Next, describe your project's structure using fl.Folder and fl.DataBase.

Paths are generated automatically based on class and attribute names.

```python
## Declare the embedded database and its tables
class Analytics(fl.DataBase):
    sales = fl.Table(SalesDB)

## Declare the root folder for our project

class MyProject(fl.Folder):
    __source__ = Path("./my_data_project")  ## Sets the root path

    ## Files are defined as attributes
    raw_sales = fl.CSV(model=SalesFile)

    ## You can nest other layouts, like our database
    analytics_db = Analytics()
```

### Use the Defined Layout

Now you can interact with your project through this clean, declarative API.

```python
## Mock some data for the example

mock_sales_data = pl.DataFrame(
    {
        "transaction_id": [101, 102, 103],
        "customer_id": [1, 2, 1],
        "amount": [120.50, 75.00, 50.25],
    }
)

## 1. Write data to the CSV, automatically casting to the `SalesFile` schema

MyProject.raw_sales.write_cast(mock_sales_data)
print(f"✅ Raw sales data written to: {MyProject.raw_sales.source}")

## 2. Scan the raw data and load it into the DuckDB database

raw_df = MyProject.raw_sales.scan_cast()

with MyProject.analytics_db as db:
    db.sales.create_or_replace_from(raw_df)
    print("✅ Data loaded into DuckDB.")

    ## 3. Query the data directly from the database using the Narwhals API
    report_df = (
        db.sales.read()
        .group_by("customer_id")
        .agg(
            total_spent=nw.sum("amount"),
            transaction_count=nw.len(),
        )
        .collect()
        .to_native()
    )
    print("\n📊 Generated Report:")
    print(report_df)

```

## Key Features

### Declarative Path Management

Paths are derived automatically from your class structure.

This eliminates brittle, hardcoded strings and makes refactoring trivial.

Inheritance can be used to create logical sub-folders.

```python
class V1(fl.Folder):
    __source__ = Path("./production_data")
    sales = fl.CSV(model=SalesFile)

class V2(V1): ## Inherits from V1
    ## This file will be located at './production_data/v2/reports.parquet'
    reports = fl.Parquet()

## The `source` attribute gives you the resolved pathlib.Path object

print(V1.sales.source)

## >>> PosixPath('production_data/sales.csv')

print(V2.reports.source)

## >>> PosixPath('production_data/v2/reports.parquet')

```

## Schema-Driven I/O

**Never guess your data types again.**

Framelib uses the attached schema to cast data during I/O operations, ensuring that your dataframes always conform to the defined contract.

- read_cast(): Reads the entire file into a Polars DataFrame and applies the schema.
- scan_cast(): Scans the file as a Polars LazyFrame and applies the schema.
- write_cast(): Casts a DataFrame to the schema before writing it to a file.

This will raise an error if the data in 'raw_sales.csv' doesn't match the SalesFile schema

```python
df = MyProject.raw_sales.read_cast()
```

Integrated Database

Go beyond flat files with the fl.DataBase layout.

 It provides a clean, high-level interface for an embedded DuckDB instance, managed as a context manager to handle connections automatically.

```python
new_sales = pl.DataFrame(...)  

with MyProject.analytics_db as db:     
## High-level methods simplify common database operations
    db.sales.append(new_sales)
    db.sales.truncate()
## Intelligently insert rows, skipping duplicates based on the primary key
    db.sales.insert_if_not_exists(new_sales)
```

## Directory & Schema Visualization

**Understand your project's structure at a glance**.

The *show_tree()* method prints a visual representation of your declared layout, while accessing the .model attribute on a file entry displays its schema.

```python
## Assuming the directories and files have been created
print(MyProject.show_tree())

```

This will output a tree structure representing your project on the file system:

```bash
my_data_project/
├── analytics_db.ddb
└── raw_sales.csv
```

## Credits

Heavily inspired by dataframely: <https://github.com/quantco/dataframely>

## License

MIT License. See [LICENSE](./LICENSE) for details.
