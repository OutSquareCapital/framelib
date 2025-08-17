## Quick orientation for AI coding agents

This repository `framelib` provides small utilities around Polars, Plotly and Dataframely to standardize schema-backed file IO and plotting helpers.

Keep the guidance below compact and concrete so you can be productive quickly in code edits, tests and docs.

### Big picture
- Package layout: `src/framelib/` is the main package. Key subpackages:
- `schemas/` — file-schema abstractions (see `src/framelib/schemas/_schemas.py` and `src/framelib/schemas/_lib.py`).
- `graphs/` and `stats/` — plotting and stats helpers (exported from `src/framelib/__init__.py`).
- Core idea: define lightweight Schema subclasses that declare `__directory__` and `__ext__` and get `.path()`, `.show_tree()`, and IO methods via `IODescriptor`.

### Important files to reference
- `pyproject.toml` — dependency list and package metadata (Python >=3.13). Use it to infer required runtime libs: `dataframely`, `plotly`, `plotly-stubs`, `pyarrow-stubs`.
- `src/framelib/schemas/_schemas.py` — Schema base, `IODescriptor`, examples: `CSV`, `Parquet`, `NDJSON`.
- `src/framelib/schemas/_lib.py` — helpers used by schemas (tree display, path helpers). Read before changing schema behavior.
- `tests/main.py` — runs package doctests across `src/`; use this to validate examples and small edits quickly.
- `README.md` — user-facing summary and install hint (`uv add ...` used in this org).

### Project-specific conventions and patterns
- Modern python (3.13+) syntax is expected, and type hints are mandatory.
- Schema pattern: subclasses of `Schema` should set `__directory__` to a path (often a tests folder like `tests/data_csv`) and `__ext__` to an extension string. The class name defines the filename.
- IODescriptor: descriptor that binds polars reader functions to the file path. It returns a callable that already has the schema path as the first argument: eg `MyFile.read()` calls `pl.read_csv(path)`.
- Tree display: `TreeDisplay` (used by `Schema.show_tree`) is the project's small utility to visualize a directory tree. Prefer using `.show_tree()` in doctests and examples.
- Tests: doctests embedded in modules are the canonical unit checks. `tests/main.py` collects and runs them. Keep doctest examples idiomatic and filesystem-local (use `tests/data_*` folders).

### Build / test / debug workflows (practical)
- Run doctests quickly from repo root:
  - `python tests/main.py` (this discovers package name from `src/` and runs doctests)
- Run linting / static checks: there is no CI config in the repo—follow local org standards; prefer running type checks with `mypy` compatible with Python 3.13 if you change types.
- Install for manual testing (project uses UV package manager in README):
  - `uv add git+https://github.com/OutSquareCapital/framelib.git` (used by repo maintainers)

### How to make safe edits
- When changing `Schema.path` logic or `IODescriptor` behaviour: update doctest examples in `src/framelib/schemas/_schemas.py` and run `python tests/main.py`.
- If you modify public APIs (exports in `src/framelib/__init__.py`), update the exported names list and run doctests.
- Preserve small, focused changes. This is a tiny utility library—keep backwards compatibility for attributes like `__directory__`, `__ext__`, and `IODescriptor` semantics.

### Examples to follow (copyable snippets)
- Define a schema in the existing style:

```python
class MyFile(CSV):
    __directory__ = "tests/data_csv"

MyFile.path().touch()
MyFile.show_tree()
```

- Add an IODescriptor-based reader if adding a new format:

```python
class XLSX(Schema):
    __ext__ = ".xlsx"
    read = IODescriptor(pl.read_excel)
```

### Integration points & external dependencies
- Dataframely: `Schema` subclasses extend `dataframely.Schema`—avoid breaking expectations from that upstream class.
- Polars: `IODescriptor` typically wraps `polars` functions (e.g., `pl.read_csv`, `pl.read_parquet`). Keep imports local to schema modules to avoid heavy imports on top-level package import.

---