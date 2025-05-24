# core/sqlite_table.py
import sqlite3
from typing import List, Dict, Any, Optional, Union
from core.base_table import BaseTable


class SQLiteTable(BaseTable):
    """
    Implementation of BaseTable using SQLite for persistent storage.
    
    Attributes:
        name: Name of the table.
        columns: List of column names.
        col_types: Mapping of column names to SQL data types.
    """

    def __init__(self, name: str, columns: List[str], col_types: Optional[Dict[str, str]] = None):
        self.name = name
        self.columns = columns
        self.col_types = col_types or {col: "TEXT" for col in columns}
        self._create_table()

    def _create_table(self):
        """Create the table if it doesn't already exist."""
        col_defs = ", ".join([f"{col} {self.col_types[col]}" for col in self.columns])
        with sqlite3.connect("db.sqlite") as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {self.name} ({col_defs})")

    def insert(self, values: List[Any]) -> None:
        """
        Insert a new row into the table.
        
        Args:
            values: List of values matching the table schema.
        
        Raises:
            ValueError: If number of values does not match number of columns.
        """
        if len(values) != len(self.columns):
            raise ValueError("Number of values must match number of columns.")
        placeholders = ", ".join(["?"] * len(values))
        with sqlite3.connect("db.sqlite") as conn:
            conn.execute(
                f"INSERT INTO {self.name} VALUES ({placeholders})", values
            )

    def select_all(self) -> List[Dict[str, Any]]:
        """
        Retrieve all rows from the table as a list of dictionaries.
        
        Returns:
            List of dictionaries representing each row.
        """
        with sqlite3.connect("db.sqlite") as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {self.name}")
            rows = cursor.fetchall()
            return [dict(zip(self.columns, row)) for row in rows]

    def update(self, set_values: Dict[str, Any], where_clause: str, params: tuple) -> None:
        """
        Update rows based on a WHERE condition.
        
        Args:
            set_values: Dictionary mapping column names to new values.
            where_clause: SQL WHERE clause (e.g., "id = ?").
            params: Tuple of parameters for WHERE clause.
        """
        set_clause = ", ".join([f"{key} = ?" for key in set_values.keys()])
        full_sql = f"UPDATE {self.name} SET {set_clause} WHERE {where_clause}"
        with sqlite3.connect("db.sqlite") as conn:
            conn.execute(full_sql, tuple(set_values.values()) + params)

    def delete(self, where_clause: str, params: tuple) -> None:
        """
        Delete rows based on a WHERE condition.
        
        Args:
            where_clause: SQL WHERE clause (e.g., "id = ?").
            params: Tuple of parameters for WHERE clause.
        """
        with sqlite3.connect("db.sqlite") as conn:
            conn.execute(f"DELETE FROM {self.name} WHERE {where_clause}", params)

    def save(self) -> None:
        """
        No-op for SQLite since changes are committed immediately.
        Included to satisfy BaseTable interface.
        """
        pass

    @staticmethod
    def load(name: str) -> "SQLiteTable":
        """
        Load an existing SQLiteTable instance from the database.
        
        Args:
            name: Name of the table to load.
        
        Returns:
            Loaded SQLiteTable instance.
        
        Raises:
            ValueError: If table doesn't exist.
        """
        with sqlite3.connect("db.sqlite") as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({name})")
            info = cursor.fetchall()
            if not info:
                raise ValueError(f"Table '{name}' does not exist in the database.")
            columns = [row[1] for row in info]
            col_types = {row[1]: row[2].upper() for row in info}
            return SQLiteTable(name, columns, col_types)

    @staticmethod
    def exists(name: str) -> bool:
        """Check if a table exists in the database."""
        with sqlite3.connect("db.sqlite") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
            return cursor.fetchone() is not None

    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Execute a custom SQL query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters as a tuple
        
        Returns:
            List of dictionaries containing query results
        
        Example:
            table.execute_query("SELECT * FROM users WHERE age > ?", (25,))
        """
        with sqlite3.connect("db.sqlite") as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            # If SELECT query, return results
            if query.strip().upper().startswith("SELECT"):
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            
            # For other queries (INSERT, UPDATE, DELETE), commit and return empty list
            conn.commit()
            return []