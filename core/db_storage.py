import sqlite3
from typing import Any, Dict, List, Optional
import json
import os

class DatabaseStorage:
    """
    Manages SQLite storage for tables and UDFs.
    """
    
    def __init__(self, db_path: str = "data/database.sqlite"):
        """
        Initialize database storage.
        
        Args:
            db_path: Path to SQLite database file
        """
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create UDF registry table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS udf_registry (
                name TEXT PRIMARY KEY,
                expression TEXT UNIQUE,
                parameters TEXT
            )
            """)
            
            # Create table registry table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS table_registry (
                name TEXT PRIMARY KEY,
                columns TEXT
            )
            """)
            
            conn.commit()
    
    def _register_sqlite_udfs(self, conn: sqlite3.Connection) -> None:
        """Register UDFs with SQLite connection."""
        cursor = conn.cursor()
        cursor.execute("SELECT name, expression, parameters FROM udf_registry")
        
        for name, expr, params in cursor.fetchall():
            params_list = json.loads(params)
            
            def create_udf(expr=expr, params=params_list):
                def udf_func(*args):
                    # Map parameters to their values
                    values = dict(zip(params, args))
                    
                    # Replace parameters with values in the expression
                    result_expr = expr
                    for param, value in values.items():
                        result_expr = result_expr.replace(param, str(value))
                    
                    # Evaluate the expression
                    return eval(result_expr)
                return udf_func
            
            # Register the UDF with SQLite
            conn.create_function(name, len(json.loads(params)), create_udf())
    
    def save_udf(self, name: str, expression: str, parameters: List[str]) -> None:
        """
        Save a UDF definition to the database.
        
        Args:
            name: Name of the UDF
            expression: The expression to evaluate
            parameters: List of parameter names
        """
        # Normalize the expression
        normalized = ' '.join(expression.split())
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check if this expression already exists
            cursor.execute("""
            SELECT name FROM udf_registry
            WHERE expression = ?
            """, (normalized,))
            
            existing = cursor.fetchone()
            if existing:
                # Update the name if needed
                if existing[0] != name:
                    cursor.execute("""
                    UPDATE udf_registry
                    SET name = ?
                    WHERE expression = ?
                    """, (name, normalized))
            else:
                # Insert new UDF
                cursor.execute("""
                INSERT OR REPLACE INTO udf_registry (name, expression, parameters)
                VALUES (?, ?, ?)
                """, (name, normalized, json.dumps(parameters)))
            
            conn.commit()
    
    def get_udf(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a UDF definition from the database.
        
        Args:
            name: UDF name
            
        Returns:
            Dictionary containing UDF details if found, None otherwise
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name, expression, parameters FROM udf_registry WHERE name = ?",
                (name,)
            )
            row = cursor.fetchone()
            
            if row:
                return {
                    "name": row[0],
                    "expression": row[1],
                    "parameters": json.loads(row[2])
                }
            return None
    
    def list_udfs(self) -> List[Dict[str, Any]]:
        """
        List all UDFs in the database.
        
        Returns:
            List of UDF definitions
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT DISTINCT name, expression, parameters
            FROM udf_registry
            """)
            
            udfs = []
            for name, expression, parameters in cursor.fetchall():
                udfs.append({
                    "name": name,
                    "expression": expression,
                    "parameters": json.loads(parameters)
                })
            
            return udfs
    
    def create_table(self, name: str, columns: List[str]) -> None:
        """
        Create a new table in the database.
        
        Args:
            name: Table name
            columns: List of column names
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Register the table
            cursor.execute(
                "INSERT OR REPLACE INTO table_registry (name, columns) VALUES (?, ?)",
                (name, json.dumps(columns))
            )
            
            # Drop existing table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS data_{name}")
            
            # Create the actual table with row_id
            column_defs = ", ".join(f"{col} TEXT" for col in columns)
            cursor.execute(f"""
            CREATE TABLE data_{name} (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                {column_defs}
            )
            """)
            
            conn.commit()
    
    def insert_row(self, table_name: str, values: List[Any]) -> None:
        """
        Insert a row into a table.
        
        Args:
            table_name: Name of the table
            values: List of values to insert
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get table columns
            cursor.execute("SELECT columns FROM table_registry WHERE name = ?", (table_name,))
            columns = json.loads(cursor.fetchone()[0])
            
            # Insert the row
            placeholders = ", ".join("?" * len(values))
            cursor.execute(
                f"INSERT INTO data_{table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                values
            )
            conn.commit()
    
    def select_all(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Select all rows from a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of dictionaries representing rows
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get table columns
            cursor.execute("SELECT columns FROM table_registry WHERE name = ?", (table_name,))
            columns = json.loads(cursor.fetchone()[0])
            
            # Select all rows
            cursor.execute(f"SELECT {', '.join(columns)} FROM data_{table_name}")
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query to execute
            
        Returns:
            List of dictionaries representing rows
        """
        with sqlite3.connect(self.db_path) as conn:
            # Register UDFs with this connection
            self._register_sqlite_udfs(conn)
            
            conn.row_factory = sqlite3.Row  # This enables column name access
            cursor = conn.cursor()
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

# Global database storage instance
db_storage = DatabaseStorage() 