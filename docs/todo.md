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
    lambda df: MySchemaFiles.my_db.create_or_replace().insert_into(df)
)
```
