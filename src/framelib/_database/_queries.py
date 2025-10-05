def where_clause(name: str, *keys: str) -> str:
    return " AND ".join(f'{name}."{key}" = _."{key}"' for key in keys)


def describe(table_name: str) -> str:
    return f"""--sql
    DESCRIBE {table_name};
    """


def show_types() -> str:
    return "SELECT * FROM duckdb_types();"


def show_tables() -> str:
    return """
    --sql 
    SHOW TABLES;
    """


def create_from(name: str) -> str:
    return f"""
    --sql
    CREATE TABLE {name} AS SELECT * FROM _;
    """


def add_primary_key(name: str, *keys: str) -> str:
    pk_cols = ", ".join(f'"{k}"' for k in keys)
    return f"""ALTER TABLE {name} ADD PRIMARY KEY ({pk_cols});"""


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


def insert_if_not_exists(name: str, *keys: str) -> str:
    where = " AND ".join(f'existing_data."{key}" = _."{key}"' for key in keys)

    return f"""
    --sql
    INSERT INTO {name}
    SELECT * FROM _
    WHERE NOT EXISTS (
        SELECT 1 FROM {name} AS existing_data WHERE {where}
    );
    """
