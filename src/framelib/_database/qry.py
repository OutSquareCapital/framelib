"""SQL queries for duckdb database operations."""

_DATA = "_"
"""Placeholder table name for duckdb scope."""


SHOW_TYPES = """--sql
SELECT * FROM duckdb_types();
    """

SHOW_TABLES = """--sql
    SHOW TABLES;
    """

SHOW_SCHEMAS = """--sql
    SELECT * FROM information_schema.schemata;
    """

SHOW_SETTINGS = """--sql
    SELECT * FROM duckdb_settings();
    """
SHOW_EXTENSIONS = """--sql
    SELECT * FROM duckdb_extensions();
    """
SHOW_VIEWS = """--sql
    SELECT * FROM duckdb_views();
    """
ALL_CONSTRAINTS = """--sql
    SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS;
    """


def drop(name: str) -> str:
    return f"""--sql
    DROP TABLE "{name}";
    """


def drop_if_exists(name: str) -> str:
    return f"""--sql
    DROP TABLE IF EXISTS "{name}";
    """


def create(name: str, schema_sql: str) -> str:
    return f"""--sql
    CREATE TABLE {name} ({schema_sql});
    """


def create_if_not_exist(name: str, schema_sql: str) -> str:
    return f"""--sql
    CREATE TABLE IF NOT EXISTS {name} ({schema_sql});
    """


def create_or_replace(name: str, schema_sql: str) -> str:
    return f"""--sql
    CREATE OR REPLACE TABLE {name} ({schema_sql});
    """


def insert_into(name: str) -> str:
    return f"""--sql
    INSERT INTO {name} SELECT * FROM {_DATA};
    """


def insert_or_replace(name: str) -> str:
    return f"""--sql
    INSERT OR REPLACE INTO {name} SELECT * FROM {_DATA};
    """


def insert_or_ignore(name: str) -> str:
    return f"""--sql
    INSERT OR IGNORE INTO {name} SELECT * FROM {_DATA};
    """


def truncate(name: str) -> str:
    return f"""--sql
    TRUNCATE TABLE {name};
    """


def summarize(name: str) -> str:
    return f"""--sql
    SUMMARIZE {name};
    """


def columns_schema(name: str) -> str:
    return f"""--sql
    SELECT *
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{name}';
    """


def constraints(name: str) -> str:
    return f"""--sql
    SELECT constraint_name, constraint_type, constraint_column_usage.column_name
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS constraint_column_usage
        USING (constraint_name, table_name)
    WHERE table_name = '{name}';
    """
