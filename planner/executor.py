from typing import List, Any, Optional
from core.json_table import JSONTable
from core.base_table import BaseTable
import operator

ops = {
    "=": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "in": lambda x, y: x in y,
    "not in": lambda x, y: x not in y
}

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

    def select(self, criteria: Optional[List[dict]] = None, columns: Optional[List[str]] = None) -> Optional[List[dict]]:
        """
        Select rows from the table based on the given criteria and selected columns.
        """
        print("columns: ", columns)
        print("criteria: ", criteria)
        if criteria is None:
            criteria = []
        if columns is None:
            columns = ['all']

        self.storage = JSONTable.load(self.table_name)

        if self.storage_type != "json":
            raise ValueError(f"Unsupported storage type: {self.storage_type}")
        if not self.storage.exists(self.table_name):
            raise ValueError(f"Table '{self.table_name}' does not exist.")
        
        print("Selecting rows with criteria:", criteria)
        all_rows = self.storage.select_all()

        # Apply filtering
        if criteria:
            filtered_rows = [
                row for row in all_rows
                if all(
                    ops[condition["operator"]](row[condition["column"]], condition["value"])
                    for condition in criteria
                    if condition["operator"] in ops and condition["column"] in row
                )
            ]
        else:
            filtered_rows = all_rows

        print("Filtering rows by columns:", columns)

        # Apply column selection
        if columns != ['all']:
            filtered_rows = [
                {col: row[col] for col in columns if col in row}
                for row in filtered_rows
            ]

        return filtered_rows if filtered_rows else None
