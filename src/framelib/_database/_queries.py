"""SQL queries for DuckDB database operations. All raw SQL is defined/generated here."""

_SEPARATOR = ", "


def _join_keys(*keys: str) -> str:
    return _SEPARATOR.join(f'"{k}"' for k in keys)


# -- DB OPERATIONS -- #
def show_types() -> str:
    return """SELECT * FROM duckdb_types();"""


def show_tables() -> str:
    return """
    --sql 
    SHOW TABLES;
    """


def show_schemas() -> str:
    return """--sql
    SHOW SCHEMAS;
    """


# -- TABLE OPERATIONS -- #
def describe(name: str) -> str:
    return f"""--sql
    DESCRIBE {name};
    """


def create_from(name: str) -> str:
    return f"""
    --sql
    CREATE TABLE {name} AS SELECT * FROM _;
    """


def add_primary_key(name: str, *keys: str) -> str:
    return f"""ALTER TABLE {name} ADD PRIMARY KEY ({_join_keys(*keys)});"""


def add_unique_key(name: str, *keys: str) -> str:
    return f"""ALTER TABLE {name} ADD UNIQUE ({_join_keys(*keys)});"""


def create_or_replace(name: str) -> str:
    return f"""
    --sql
    CREATE OR REPLACE TABLE {name} AS SELECT * FROM _;
    """


def insert_into(name: str) -> str:
    return f"""
    --sql
    INSERT INTO {name} SELECT * FROM _;
    """


def drop(name: str) -> str:
    return f"""
    --sql
    DROP TABLE IF EXISTS {name};
    """


def truncate(name: str) -> str:
    return f"""
    --sql
    TRUNCATE TABLE {name};
    """


def insert_or_replace(name: str) -> str:
    return f"""
    --sql
    INSERT OR REPLACE INTO {name} SELECT * FROM _;
    """


def insert_or_ignore(name: str) -> str:
    return f"""
    --sql
    INSERT OR IGNORE INTO {name} SELECT * FROM _;
    """


def summarize(table_name: str) -> str:
    return f"""
    --sql 
    SUMMARIZE {table_name};
    """
