from dataclasses import dataclass
from enum import StrEnum

import pychain as pc

_DATA = "_"
"""Placeholder table name for duckdb scope."""


class DBQueries(StrEnum):
    """General SQL queries not tied to a specific table."""

    SHOW_TYPES = """SELECT * FROM duckdb_types();"""

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


def drop_table(name: str) -> str:
    return f"""
    --sql
    DROP TABLE IF EXISTS "{name}";
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

    def create_or_replace(self, schema_sql: str) -> str:
        return f"""
        --sql
        CREATE OR REPLACE TABLE {self.name} ({schema_sql});
        """

    def insert_into(self) -> str:
        return f"""
        --sql
        INSERT INTO {self.name} SELECT * FROM {_DATA};
        """

    def drop(self) -> str:
        return drop_table(self.name)

    def truncate(self) -> str:
        return f"""
        --sql
        TRUNCATE TABLE {self.name};
        """

    def insert_or_replace(self) -> str:
        return f"""
        --sql
        INSERT OR REPLACE INTO {self.name} SELECT * FROM {_DATA};
        """

    def insert_or_ignore(self) -> str:
        return f"""
        --sql
        INSERT OR IGNORE INTO {self.name} SELECT * FROM {_DATA};
        """

    def summarize(self) -> str:
        return f"""
        --sql 
        SUMMARIZE {self.name};
        """

    def insert_on_conflict_update(
        self, columns: pc.Iter[str], conflict_keys: list[str]
    ) -> str:
        update_keys: pc.Iter[str] = columns.filter_notin(conflict_keys)
        conflict_target: str = (
            pc.Iter(conflict_keys)
            .map(lambda k: f'"{k}"')
            .into(lambda ks: f"({', '.join(ks)})")
        )

        if not update_keys.unwrap():
            return f"""
            --sql
            INSERT INTO {self.name} SELECT * FROM {_DATA}
            ON CONFLICT {conflict_target} DO NOTHING;
            """

        update_clause: str = update_keys.map(
            lambda col: f'"{col}" = excluded."{col}"'
        ).into(", ".join)

        return f"""
        --sql
        INSERT INTO {self.name} SELECT * FROM {_DATA}
        ON CONFLICT {conflict_target} DO UPDATE SET {update_clause};
        """

    def columns_schema(self) -> str:
        return f"""
        --sql
        SELECT *
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{self.name}';
        """

    def constraints(self) -> str:
        return f"""
        --sql
        SELECT constraint_name, constraint_type, constraint_column_usage.column_name
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS constraint_column_usage
            USING (constraint_name, table_name)
        WHERE table_name = '{self.name}';
        """
