# TODO

## List of tasks to do

### Debugging

- Fix insert_on_update bugs (messy code, does not work ATM).
- Fix init_subclass process (no strict control ATM, no clear schemas nor error messages).

### Features

- Add better support for folder/files management (glob patterns, recursive search, etc...).
- Check dataframely for features to implement (validations, collections, etc...).
- Check all duckdb python API methods/function, see if some are missing, potentially replace raw SQL queries with API calls, add missing features.

### Improvements

- Use slots everywhere if possible.
- Improve all __repr__ methods (use tree? what do to with schema vs entries?)
- Better handle DataBase and Table API (TypeState pattern to avoid calling methods outside of apply/pipe context?).

### Code architecture

- Analyse the current inheritance structure of Columns, FileHandlers, Schemas, etc... and see if it can be improved/simplified.

### Need Decisions

- Decide on how to manage file readers/scanners/writers (partials? automatic script who creates them? custom implementation?).
- Decide how to handle Enum/Categorical for duckdb (still return VARCHAR? special management?).
- Where should we check for schema compliance? On FileHandler read/write? On Schema creation? On Column creation?
- Should we keep the current implicit path creation from class names? How should we handle config?
- Should we handle data migration more explicitly?

### Tests

- Refactor tests to cover all cases.

### Documentation

- Deploy documentation site (see pyochain docs deployment).
- Find a way to better review and maintain documentation at repo level (TODO updates, code architecture diagrams, etc...).

## Details

### Schema migration strategy

Currently this is both straigthforward and complicated.
Since a schema is agnostic to it's model handler (File or Table), we can easily change the underlying storage without changing the schema. However, going from a File to a Table is not explicitely managed, even tough it's just a chain of:

```python
MySchemaFiles.my_file.read().pipe(
    lambda df: MySchemaFiles.my_db.apply(lambda db: db.my_db.create_or_replace_from(df)),
)
```
