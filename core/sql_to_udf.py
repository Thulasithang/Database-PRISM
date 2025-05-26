import re
from typing import List, Dict, Any, Callable, Optional, Tuple
import ast
from core.sqlite_table import SQLiteTable

class SQLToUDF:
    """Utility class to convert SQL calculations into reusable UDFs."""
    
    def __init__(self, table: SQLiteTable):
        self.table = table
        self.udfs: Dict[str, Tuple[str, Callable]] = {}
    
    def extract_calculation(self, sql_query: str, calc_name: str) -> str:
        """
        Extract a calculation from a SQL query and create a UDF.
        
        Args:
            sql_query: The SQL query containing the calculation
            calc_name: Name to give to the extracted calculation function
            
        Returns:
            Generated Python code for the UDF
        """
        # Extract the calculation part (typically between SELECT and FROM)
        calc_match = re.search(
            r'SELECT\s+.*?(ROUND\s*\(.*?\)|SUM\s*\(.*?\)|AVG\s*\(.*?\)|\w+\s*\(.*?\))\s+(?:as\s+\w+\s+)?FROM',
            sql_query,
            re.IGNORECASE | re.DOTALL
        )
        
        if not calc_match:
            raise ValueError("No calculation found in the query")
            
        calc_expr = calc_match.group(1)
        
        # Convert SQL calculation to Python code
        python_code = self._sql_to_python(calc_expr, calc_name)
        
        # Save the UDF
        self._save_udf(calc_name, python_code)
        
        return python_code
    
    def _sql_to_python(self, sql_expr: str, func_name: str) -> str:
        """Convert SQL expression to Python function code."""
        # Clean up SQL expression
        sql_expr = sql_expr.strip()
        
        # Replace SQL functions with Python equivalents
        replacements = {
            r'ROUND\s*\((.*?),\s*(\d+)\)': r'round(\1, \2)',
            r'SUM\s*\((.*?)\)': r'sum([\1])',
            r'AVG\s*\((.*?)\)': r'sum([\1])/len([\1])',
            r'COUNT\s*\((.*?)\)': r'len([\1])',
            r'MAX\s*\((.*?)\)': r'max([\1])',
            r'MIN\s*\((.*?)\)': r'min([\1])'
        }
        
        python_expr = sql_expr
        for sql_pattern, py_pattern in replacements.items():
            python_expr = re.sub(sql_pattern, py_pattern, python_expr, flags=re.IGNORECASE)
        
        # Create function template
        func_template = f'''
def {func_name}(*args) -> float:
    """
    UDF generated from SQL calculation.
    Args will be passed in the order they appear in the original SQL query.
    """
    try:
        result = {python_expr}
        return float(result)
    except Exception as e:
        print(f"Error in {func_name}: {{e}}")
        return 0.0
'''
        return func_template
    
    def _save_udf(self, func_name: str, python_code: str) -> None:
        """Save the UDF to a Python file."""
        file_name = f"generated_udfs/{func_name}.py"
        
        # Create directory if it doesn't exist
        import os
        os.makedirs("generated_udfs", exist_ok=True)
        
        # Save the code
        with open(file_name, "w") as f:
            f.write(python_code)
        
        # Also save to our internal dictionary
        # Execute the code to get the function object
        namespace = {}
        exec(python_code, namespace)
        self.udfs[func_name] = (python_code, namespace[func_name])
        
        # Register with SQLite
        self.table.register_udf(func_name, self._count_params(python_code), namespace[func_name])
    
    def _count_params(self, python_code: str) -> int:
        """Count the number of parameters in the function definition."""
        tree = ast.parse(python_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Count *args as 1 parameter since we'll pass arrays
                return 1
        return 0
    
    def load_udf(self, func_name: str) -> Optional[Callable]:
        """
        Load a previously saved UDF.
        
        Args:
            func_name: Name of the UDF to load
            
        Returns:
            The loaded function object, or None if not found
        """
        file_name = f"generated_udfs/{func_name}.py"
        if not os.path.exists(file_name):
            return None
            
        with open(file_name, "r") as f:
            python_code = f.read()
            
        # Execute the code to get the function object
        namespace = {}
        exec(python_code, namespace)
        
        # Save to our dictionary and register with SQLite
        self.udfs[func_name] = (python_code, namespace[func_name])
        self.table.register_udf(func_name, self._count_params(python_code), namespace[func_name])
        
        return namespace[func_name]
    
    def get_udf_code(self, func_name: str) -> Optional[str]:
        """Get the source code of a saved UDF."""
        if func_name in self.udfs:
            return self.udfs[func_name][0]
        return None
    
    def list_udfs(self) -> List[str]:
        """List all available UDFs."""
        return list(self.udfs.keys()) 