# TODO

## List of tasks to do

### Debugging

- Fix insert_on_update bugs (messy code, does not work ATM).
- Fix init_subclass process (no strict control ATM, no clear schemas nor error messages).

### API

- Decide how `__source__` should be handled for custom set -> manual attribute or decorator?

### Features

- Check dataframely for features to implement on Schemas (validations, collections, etc...).
- Check all duckdb python API methods/function, see if some are missing, potentially replace raw SQL queries with API calls, add missing features.
- Foreign keys
- Check constraint duckdb
- default duckdb
- Views
- Transactions
- Conditional inserts/updates

### Improvements

- Improve all __repr__ methods (use tree? what do to with schema vs entries?)
- Add a "light", pure python Schema/Column/FileHandler version with lazy file reading and reliance on pyochain Iterators for data processing. Probably must continue to improve dictexpr for that. tinyDb could be a good inspiration, also this <https://github.com/jazzband/jsonmodels>.
- Use Result and Option across the codebase to make errors explicit
- Decide how to handle "initial setup" scenarios (creating folders, databases, etc...). subclass init, decorator?
- Check how to handle prefixes for files paths (e.g ./data/, s3://, etc...) in a clean way.

### Code architecture

- Analyse the current inheritance structure of Columns, FileHandlers, Schemas, etc... and see if it can be improved/simplified.
- Related to above, decide how to handle relationship of DataBase and Folder. Should DataBase be completely independent if wanted?

### Need Decisions

- Decide on how to manage file readers/scanners/writers (partials? automatic script who creates them? custom implementation?).
- Decide how to handle Enum/Categorical for duckdb (still return VARCHAR? special management?).
- Where should we check for schema compliance? On FileHandler read/write? On Schema creation? On Column creation?
- Should we keep the current implicit path creation from class names? How should we handle config?
- Should we handle data migration more explicitly?

### Documentation

- Deploy documentation site (see pyochain docs deployment).
- Find a way to better review and maintain documentation at repo level (TODO updates, code architecture diagrams, etc...).

## Details

### Schema migration strategy

Currently this is both straigthforward and complicated.
Since a schema is agnostic to it's handler (File or Table), we can easily change the underlying storage without changing the schema. However, going from a File to a Table is not explicitely managed, even tough it's just a chain of:

```python
MySchemaFiles.my_file.read().pipe(
    lambda df: MySchemaFiles.my_db.create_or_replace().insert_into(df)
)
```

## Planned Query Compilation Layer (Polars-like SQL for DuckDB)

This section summarizes the current design direction for query execution discussed during development.

### Goal

Provide a __Polars-like__ user experience while keeping evaluation lazy for as long as possible, without
forcing an early conversion such as calling an equivalent of `.to_native().pl(lazy=True)` to circumvent narwhals limitations.

### Problem

Currently narwhals does not support all duckdb possibilities, and the user experience is often met with limitations and runtime surprises.
Narwhals is:

- Primarily for library creators, not script end-users.
- Limited by design to a common denominator of multiple backends, missing duckdb-specific optimizations and features.

This force the user to call `to_native().pl(lazy=True)` to split the query in two parts, or to reimplement the logic in a less polars-idiomatic way,  when incompatible operations are used.

Even assuming an efficient `duckdb.PyRelation.pl(lazy=True)` situation for the caller, this would still separate the query in two, missing optimizations opportunities.

Assuming an incompatible conversion (slow path), this is catastrophic for performance.

### Solution

The core idea is to introduce a dedicated, explicit polars-like DSL for SQL targeting Duckdb.

This design consists of three main layers (current ideas):

1. __Front-end (Polars-like DSL)__
    - Expose a Polars-like expression and frame API (similar to Polars Expr/Frame).
    - Mirror the latest up-to-date Polars API as closely as possible.

2. __Middle layer (IR as the source of truth)__
     - Build an internal IR using Sqlglot.

3. __Backend (DuckDB SQL)__
    - Pass the generated SQL to DuckDB for execution.

### Relationship with Narwhals and Polars SQL

- __Narwhals__: useful as a compatibility layer (multi-backend surface) but not a good foundation
    for "Polars-like -> SQL" compilation. Due to it's multi-support design, it cannot cover the full polars API for duckdb, and impose artificial constraints.
