from dataclasses import dataclass
from enum import StrEnum

_SEPARATOR = ", "
DATA = "_"


def _join_keys(*keys: str) -> str:
    return _SEPARATOR.join(f'"{k}"' for k in keys)


class DBQueries(StrEnum):
    SHOW_TYPES = """SELECT * FROM duckdb_types();"""

    SHOW_TABLES = """--sql 
        SHOW TABLES;
        """

    SHOW_SCHEMAS = """--sql
        SHOW SCHEMAS;
        """


@dataclass(slots=True, frozen=True)
class Queries:
    """SQL queries builder for a specific table."""

    name: str

    def create_from(self) -> str:
        return f"""
        --sql
        CREATE TABLE {self.name} AS SELECT * FROM _;
        """

    def add_primary_key(self, *keys: str) -> str:
        return f"""ALTER TABLE {self.name} ADD PRIMARY KEY ({_join_keys(*keys)});"""

    def add_unique_key(self, *keys: str) -> str:
        return f"""ALTER TABLE {self.name} ADD UNIQUE ({_join_keys(*keys)});"""

    def create_or_replace(self, schema_sql: str) -> str:
        return f"""
        --sql
        CREATE OR REPLACE TABLE {self.name} ({schema_sql});
        """

    def insert_into(self) -> str:
        return f"""
        --sql
        INSERT INTO {self.name} SELECT * FROM {DATA};
        """

    def drop(self) -> str:
        return f"""
        --sql
        DROP TABLE IF EXISTS {self.name};
        """

    def truncate(self) -> str:
        return f"""
        --sql
        TRUNCATE TABLE {self.name};
        """

    def insert_or_replace(self) -> str:
        return f"""
        --sql
        INSERT OR REPLACE INTO {self.name} SELECT * FROM {DATA};
        """

    def insert_or_ignore(self) -> str:
        return f"""
        --sql
        INSERT OR IGNORE INTO {self.name} SELECT * FROM {DATA};
        """

    def summarize(self) -> str:
        return f"""
        --sql 
        SUMMARIZE {self.name};
        """
