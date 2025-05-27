# core/json_table.py
import json
import os
from typing import List, Dict, Any
from core.base_table import BaseTable


class JSONTable(BaseTable):
    """
    Implementation of BaseTable using JSON files for persistent storage.
    
    Attributes:
        name: Name of the table.
        columns: List of column names.
        rows: List of rows, each represented as a list of values.
    """

    def __init__(self, name: str, columns: List[str]):
        self.name = name
        self.columns = columns
        self.rows: List[List[Any]] = []

    def insert(self, values: List[Any]) -> None:
        """
        Insert a new row into the table.
        
        Args:
            values: List of values matching the table schema.
        
        Raises:
            ValueError: If number of values does not match number of columns.
        """
        print(f"Inserting values into table '{self.name}': {values}")
        if len(values) != len(self.columns):
            raise ValueError("Number of values must match number of columns.")
        self.rows.append(values)

    def select_all(self) -> List[Dict[str, Any]]:
        """
        Retrieve all rows from the table as a list of dictionaries.
        
        Returns:
            List of dictionaries representing each row.
        """
        return [dict(zip(self.columns, row)) for row in self.rows]

    def save(self, base_path: str = "data") -> None:
        """
        Save the table data to a JSON file.
        
        Args:
            base_path: Directory where JSON files are stored.
        """
        os.makedirs(base_path, exist_ok=True)
        filepath = os.path.join(base_path, f"{self.name}.json")
        print("rows to add: ", self.rows)
        with open(filepath, "w") as f:
            json.dump({
                "name": self.name,
                "columns": self.columns,
                "rows": self.rows
            }, f, indent=2)

    @staticmethod
    def load(name: str, base_path: str = "data") -> "JSONTable":
        """
        Load a JSONTable instance from a file.
        
        Args:
            name: Name of the table to load.
            base_path: Directory where JSON files are stored.
        
        Returns:
            Loaded JSONTable instance.
        
        Raises:
            FileNotFoundError: If the specified file doesn't exist.
        """
        filepath = os.path.join(base_path, f"{name}.json")
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"No table found at {filepath}")
        with open(filepath, "r") as f:
            data = json.load(f)
        table = JSONTable(data["name"], data["columns"])
        table.rows = data["rows"]
        return table

    @staticmethod
    def exists(name: str, base_path: str = "data") -> bool:
        """
        Check if a JSON file for the table exists.
        
        Args:
            name: Name of the table to check.
            base_path: Directory where JSON files are stored.
        
        Returns:
            True if the file exists, False otherwise.
        """
        filepath = os.path.join(base_path, f"{name}.json")
        return os.path.exists(filepath)