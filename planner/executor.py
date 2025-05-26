from typing import List, Any
from core.json_table import JSONTable
from core.base_table import BaseTable

class Executor:
    def __init__(self, table_name: str, storage_type: str = "json"):
        """
        Initialize the Executor with a storage backend and table name.
        
        Args:
            storage: An instance of BaseTable or its subclass for data operations.
            table_name: Name of the table to operate on.
        """
        self.storage_type = storage_type
        self.table_name = table_name

    def create_table(self, columns: List[str]) -> None:
        """
        Create a new table with the specified columns.

        Args:
            columns: List of column names for the new table.
        """
        if self.storage_type == "json":
            self.storage = JSONTable(self.table_name, columns)
        else:
            raise ValueError(f"Unsupported storage type: {self.storage_type}")
        
        if not self.storage.exists(self.table_name):
            self.storage.save()
        

    def insert(self, values: List[Any]) -> None:
        """
        Insert a new row into the table.

        Args:
            values: List of values to insert into the table.
        """
        self.storage = JSONTable.load(self.table_name)
        if self.storage_type != "json":
            raise ValueError(f"Unsupported storage type: {self.storage_type}")
        if not self.storage.exists(self.table_name):
            raise ValueError(f"Table '{self.table_name}' does not exist.")
        print("Inserting values into table:", values)
        table = JSONTable(self.table_name, self.storage.columns)
        table.insert(values)
        table.save()
        # self.storage.insert(values)

    def select(self, criteria: dict) -> List[dict]:
        """
        Select rows from the table based on the given criteria.

        Args:
            criteria: Dictionary of column names and values to filter by.

        Returns:
            List of rows matching the criteria.
        """
        if not self.storage.exists(self.table_name):
            raise ValueError(f"Table '{self.table_name}' does not exist.")
        rows = self.storage.scan(self.table_name)
        return self.filter_and_project(rows, criteria)

        