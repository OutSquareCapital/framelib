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

A marimo notebook with more detailed examples is available at <https://static.marimo.app/static/example-atk1>

## Credits

Heavily inspired by dataframely: <https://github.com/quantco/dataframely>

## License

MIT License. See [LICENSE](./LICENSE) for details.
