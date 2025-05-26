import os
import json
from typing import Dict, List, Any

class TableManager:
    """Manages table storage and operations using JSON files."""
    
    def __init__(self, data_dir: str = "data/tables"):
        """Initialize the table manager with a data directory."""
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.tables: Dict[str, Dict] = self._load_tables()
        
    def _load_tables(self) -> Dict[str, Dict]:
        """Load all existing tables from the data directory."""
        tables = {}
        if os.path.exists(self.data_dir):
            for filename in os.listdir(self.data_dir):
                if filename.endswith('.json'):
                    table_name = filename[:-5]  # Remove .json
                    filepath = os.path.join(self.data_dir, filename)
                    with open(filepath, 'r') as f:
                        tables[table_name] = json.load(f)
        return tables
        
    def create_table(self, name: str, columns: List[Dict[str, str]]) -> None:
        """
        Create a new table.
        
        Args:
            name: Table name
            columns: List of column definitions [{"name": str, "datatype": str}]
        """
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists")
            
        table_data = {
            "name": name,
            "columns": columns,
            "rows": []
        }
        
        # Save to memory
        self.tables[name] = table_data
        
        # Save to disk
        filepath = os.path.join(self.data_dir, f"{name}.json")
        with open(filepath, 'w') as f:
            json.dump(table_data, f, indent=2)
            
    def insert_into(self, table_name: str, columns: List[str], values: List[Any]) -> None:
        """
        Insert a row into a table.
        
        Args:
            table_name: Name of the table
            columns: List of column names
            values: List of values to insert
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
            
        table = self.tables[table_name]
        
        # Validate columns
        table_columns = {col["name"]: col["datatype"] for col in table["columns"]}
        for col in columns:
            if col not in table_columns:
                raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
                
        # Create a full row with all columns (NULL for missing values)
        row = [None] * len(table["columns"])
        for col, val in zip(columns, values):
            # Find column index
            for i, col_def in enumerate(table["columns"]):
                if col_def["name"] == col:
                    # Validate data type
                    if not self._validate_type(val, col_def["datatype"]):
                        raise ValueError(f"Invalid value type for column '{col}': expected {col_def['datatype']}")
                    row[i] = val
                    break
                    
        # Add row to table
        table["rows"].append(row)
        
        # Save to disk
        filepath = os.path.join(self.data_dir, f"{table_name}.json")
        with open(filepath, 'w') as f:
            json.dump(table, f, indent=2)
            
    def select_from(self, table_name: str, columns: List[str] = None, where: Dict = None) -> List[Dict[str, Any]]:
        """
        Select data from a table.
        
        Args:
            table_name: Name of the table
            columns: List of columns to select (None for all)
            where: Where clause conditions
            
        Returns:
            List of rows as dictionaries
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
            
        table = self.tables[table_name]
        table_columns = [col["name"] for col in table["columns"]]
        column_types = {col["name"]: col["datatype"] for col in table["columns"]}
        
        # If no columns specified, select all
        if not columns:
            columns = table_columns
            
        # Validate requested columns
        for col in columns:
            if col not in table_columns:
                raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
                
        # Convert rows to dictionaries
        result = []
        for row in table["rows"]:
            row_dict = {}
            for col_name, value in zip(table_columns, row):
                # Convert value to the correct type
                if value is not None:
                    col_type = column_types[col_name]
                    if col_type == "INT":
                        value = int(value)
                    elif col_type == "FLOAT":
                        value = float(value)
                row_dict[col_name] = value
            
            # Apply where clause if present
            if where:
                if not self._evaluate_where(where, row_dict):
                    continue
                    
            # Select only requested columns
            result.append({col: row_dict[col] for col in columns})
            
        return result
        
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate that a value matches the expected SQL type."""
        if value is None:
            return True
            
        if expected_type == "INT":
            return isinstance(value, int)
        elif expected_type == "FLOAT":
            return isinstance(value, (int, float))
        elif expected_type == "TEXT":
            return isinstance(value, str)
        return False
        
    def _evaluate_where(self, where: Dict, row: Dict[str, Any]) -> bool:
        """Evaluate a where clause against a row."""
        if not where:
            return True
            
        left = where["left"]
        op = where["op"]
        right = where["right"]
        
        # Get left value (could be column name or literal)
        left_val = row.get(left, left) if isinstance(left, str) else left
        
        # Compare values
        if op == "=":
            return left_val == right
        elif op == "!=":
            return left_val != right
        elif op == ">":
            return left_val > right
        elif op == "<":
            return left_val < right
        elif op == ">=":
            return left_val >= right
        elif op == "<=":
            return left_val <= right
        else:
            raise ValueError(f"Unsupported operator: {op}")
            
    def table_exists(self, name: str) -> bool:
        """Check if a table exists."""
        return name in self.tables
        
    def get_table_schema(self, name: str) -> List[Dict[str, str]]:
        """Get the schema of a table."""
        if name not in self.tables:
            raise ValueError(f"Table '{name}' does not exist")
        return self.tables[name]["columns"] 