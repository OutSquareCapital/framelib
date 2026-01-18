# Framelib: Declarative Data Architecture

Framelib provides a unified way to define and manage your entire data architecture—files, folders, schemas, and embedded databases—as Python classes.

Instead of hardcoding paths, managing connections manually, and validating data types inconsistently, you declare your data structure once and reuse it everywhere. Framelib leverages **pathlib**, **polars**, **narwhals**, and **duckdb** to handle the details.

## Key Features

- **Declarative Architecture**: Define your entire project structure as Python classes. Paths, schemas, and connections are inferred and consistent.
- **Type-Safe Schemas**: Define data contracts upfront with strong typing. Validation happens automatically when reading/writing.
- **Unified Data Access**: Read from CSV, Parquet, JSON files or DuckDB tables using the same simple API.
- **Automatic Connection Management**: Use databases as decorators or context managers. Connections open and close automatically.
- **Narwhals Integration**: Query your DuckDB database with `polars` syntax or raw SQL. Get back a `narwhals` LazyFrame, and convert to a `duckdb` relation with a simple call to the method `.to_native()`.
- **Project Visualization**: Automatically generate a tree view of your entire data structure.

## Installation

```bash
uv add git+https://github.com/OutSquareCapital/framelib.git
```

## Quick Start

```python
import duckdb
import polars as pl

import framelib as fl


# Define your schema
class UserSchema(fl.Schema):
    user_id = fl.UInt16(primary_key=True)
    name = fl.String()
    age = fl.UInt8()
    country = fl.Categorical()


def _create_sample_data() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "user_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 32, 46],
            "country": ["US", "UK", "CA"],
        }
    )


# Define a database with tables
class MyDatabase(fl.DataBase):
    users = fl.Table(model=UserSchema)


# Define your project structure with files
class MyProject(fl.Folder):
    users_csv = fl.CSV(model=UserSchema)
    db = MyDatabase()


# ========== File Operations ==========
# Write and read data from files
def _file_operations(df: pl.DataFrame) -> None:
    MyProject.users_csv.write(df)
    result = MyProject.users_csv.scan().select(UserSchema.age.pl_col.sum()).collect()
    print("Sum of values:", result)


# ========== Database Operations ==========
# Use the database with the decorator pattern
@MyProject.db  # <- Connection opens automatically with context manager
def _load_and_analyze(df: pl.DataFrame) -> None:
    MyProject.db.users.create_or_replace_from(df)
    print(
        "Users loaded:", MyProject.db.users.read()
    )  # Read as polars DataFrame for convenience
    # Query the database
    # Uses polars API with duckdb backend thanks to narwhal
    # Note that framelib reexport narwhals col function for convenience
    res_narwhals: duckdb.DuckDBPyRelation = (
        MyProject.db.users.scan().filter(fl.col("users") > 15).to_native()
    )
    # Execute raw SQL if you prefer
    res_sql: duckdb.DuckDBPyRelation = MyProject.db.sql(
        "SELECT * FROM users WHERE value > 15"
    ).to_native()
    assert res_narwhals.pl().equals(res_sql.pl())
    print("Filtered users:", res_narwhals)
    # Connection closes automatically when function returns, even on error


def _as_context(df: pl.DataFrame) -> None:
    # Use the database as a context manager
    with MyProject.db as db:
        db.users.insert_into(df.head(1))  # Insert polars DataFrame in Duckdb table


def main() -> None:
    df = _create_sample_data()
    _file_operations(df)
    _load_and_analyze(df)
    _as_context(df)
    # Display project structure (assuming already created files)
    MyProject.show_tree()
    # Output:
    # myproject
    # └── users_csv.csv
    # └── mydatabase.db

```

## Why Framelib?

### No More Hardcoded Paths

Typically, you have code scattered across your project hardcoding file paths and database connections:

```python
from pathlib import Path
import duckdb

csv_path = Path("data/sales.csv")
parquet_path = Path("data/sales.parquet")
db_connection = duckdb.connect("warehouse.db")
```

This is error-prone, often duplicated, and hard to maintain. With Framelib, paths are inferred from your class hierarchy and automatically consistent across your codebase.

### Type Safety From The Start

Data inconsistencies are caught early. Define a schema once with `fl.Schema`, and every operation validates against it. When you read a CSV, it's automatically cast to your schema. When you query a database, results conform to your defined types. No surprises in production.

### Unified API Regardless of Format

Whether your data lives in a CSV, Parquet file, or DuckDB table, the API is identical: `.scan()`, `.read()`, `.write()`. You don't need to remember different APIs for different data sources. Switch formats without rewriting your logic.

### Automatic Connection Lifecycle

Managing database connections manually is tedious and error-prone. With Framelib's decorator or context manager, connections open when you enter, close when you exit—even if an error occurs. No leaked connections, no manual cleanup.

### Querying Like You Code

Use the rich polars query syntax to query your DuckDB database instead of writing raw SQL. Narwhals bridges the gap, letting you write expressions that work across different backends. If you prefer SQL, just use `.sql()` directly.

### Single Source of Truth

Your data structure is defined in one place. Documentation, validation, type hints, and runtime behavior all stem from the same schema definition. Changes propagate everywhere automatically.

## Credits

Heavily inspired by dataframely: <https://github.com/quantco/dataframely>
