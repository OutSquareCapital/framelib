from dataclasses import dataclass
from enum import StrEnum

_DATA = "_"
"""Placeholder table name for duckdb scope."""


class DBQueries(StrEnum):
    """General SQL queries not tied to a specific table."""

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
        self, conflict_keys: list[str], update_keys: list[str]
    ) -> str:
        conflict_target: str = f"({', '.join(f'"{k}"' for k in conflict_keys)})"
        if not update_keys:
            return f"""
            --sql
            INSERT INTO {self.name} SELECT * FROM {_DATA}
            ON CONFLICT {conflict_target} DO NOTHING;
            """

        update_clause: str = ", ".join(
            f'"{col}" = excluded."{col}"' for col in update_keys
        )
        return f"""
        --sql
        INSERT INTO {self.name} SELECT * FROM {_DATA}
        ON CONFLICT {conflict_target} DO UPDATE SET {update_clause};
        """
