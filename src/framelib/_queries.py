def where_clause(name: str, *keys: str) -> str:
    return " AND ".join(f'{name}."{key}" = _."{key}"' for key in keys)


def create_from(name: str) -> str:
    return f"""
    --sql
    CREATE TABLE {name} AS SELECT * FROM _;
    """


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
