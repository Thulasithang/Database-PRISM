import re
from typing import List, Dict, Any, Callable, Optional, Tuple, Union
import ast
import os
from core.sqlite_table import SQLiteTable

class CalculationToUDF:
    """Utility class to convert any SQL calculation into reusable UDFs."""
    
    def __init__(self, table: Optional[SQLiteTable] = None):
        self.table = table
        self.udfs: Dict[str, Tuple[str, Callable]] = {}
        self._load_existing_udfs()
        
    def _load_existing_udfs(self):
        """Load any existing UDFs from the generated_udfs directory."""
        if not os.path.exists("generated_udfs"):
            return
            
        for file_name in os.listdir("generated_udfs"):
            if file_name.endswith(".py"):
                func_name = file_name[:-3]  # Remove .py extension
                self.load_udf(func_name)

    def create_udf(self, calculation: str, func_name: str, input_columns: List[str]) -> str:
        """
        Convert any calculation into a UDF.
        
        Args:
            calculation: The calculation expression (can be SQL or mathematical)
            func_name: Name for the generated function
            input_columns: List of column names used in the calculation
            
        Returns:
            Generated Python code for the UDF
        """
        # Clean the calculation string
        calc = self._clean_calculation(calculation)
        
        # Convert to Python expression
        python_expr = self._to_python_expr(calc)
        
        # Create the function code
        python_code = self._generate_function_code(func_name, python_expr, input_columns)
        
        # Save and register the UDF
        self._save_udf(func_name, python_code)
        
        return python_code

    def _clean_calculation(self, calc: str) -> str:
        """Clean and standardize the calculation string."""
        # Remove any 'SELECT', 'AS', etc. if present
        calc = re.sub(r'^\s*SELECT\s+', '', calc, flags=re.IGNORECASE)
        calc = re.sub(r'\s+AS\s+\w+\s*$', '', calc, flags=re.IGNORECASE)
        
        # Remove any trailing 'FROM' clause
        calc = re.sub(r'\s+FROM\s+.*$', '', calc, flags=re.IGNORECASE)
        
        return calc.strip()

    def _to_python_expr(self, expr: str) -> str:
        """Convert SQL/mathematical expression to Python expression."""
        # SQL function replacements
        sql_replacements = {
            r'ROUND\s*\((.*?),\s*(\d+)\)': r'round(\1, \2)',
            r'SUM\s*\((.*?)\)': r'sum([\1])',
            r'AVG\s*\((.*?)\)': r'sum([\1])/len([\1]) if len([\1]) > 0 else 0',
            r'COUNT\s*\((.*?)\)': r'len([\1])',
            r'MAX\s*\((.*?)\)': r'max([\1]) if len([\1]) > 0 else 0',
            r'MIN\s*\((.*?)\)': r'min([\1]) if len([\1]) > 0 else 0',
            r'ABS\s*\((.*?)\)': r'abs(\1)',
            r'POWER\s*\((.*?),\s*(.*?)\)': r'pow(\1, \2)',
            r'SQRT\s*\((.*?)\)': r'sqrt(\1)',
            r'LOG\s*\((.*?)\)': r'log(\1)',
            r'EXP\s*\((.*?)\)': r'exp(\1)',
            r'CEIL\s*\((.*?)\)': r'ceil(\1)',
            r'FLOOR\s*\((.*?)\)': r'floor(\1)',
            # Add more SQL functions as needed
        }
        
        # Mathematical operator replacements
        math_replacements = {
            r'\bdiv\b': '/',  # Integer division in some SQL dialects
            r'\bmod\b': '%',  # Modulo in some SQL dialects
            r'\^': '**',      # Power operator
        }
        
        result = expr
        
        # Apply SQL function replacements
        for pattern, replacement in sql_replacements.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
            
        # Apply mathematical operator replacements
        for pattern, replacement in math_replacements.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
            
        return result

    def _generate_function_code(self, func_name: str, python_expr: str, input_columns: List[str]) -> str:
        """Generate the complete Python function code."""
        # Import statements needed for mathematical operations
        imports = """import math
from math import sqrt, pow, log, exp, ceil, floor
from typing import List, Union, Optional
"""
        
        # Parameter definitions
        params = ", ".join(input_columns)
        param_types = ", ".join(["Union[List[float], float]"] * len(input_columns))
        
        # Function template
        template = f'''{imports}

def {func_name}({params}) -> float:
    """
    Generated UDF for calculation: {python_expr}
    
    Args:
        {chr(10).join(f'{col}: List[float] or float - Input values for {col}' for col in input_columns)}
    
    Returns:
        float: Result of the calculation
    """
    try:
        # Convert single values to lists if necessary
        inputs = []
        for arg in [{params}]:
            if isinstance(arg, (int, float)):
                inputs.append([float(arg)])
            else:
                inputs.append([float(x) for x in arg])
        
        # Assign inputs to named variables
        {chr(10).join(f'{col} = inputs[{i}]' for i, col in enumerate(input_columns))}
        
        # Perform calculation
        result = {python_expr}
        
        # Handle scalar vs array results
        if isinstance(result, (list, tuple)):
            return float(result[0]) if result else 0.0
        return float(result)
    except Exception as e:
        print(f"Error in {func_name}: {{e}}")
        return 0.0
'''
        return template

    def _save_udf(self, func_name: str, python_code: str) -> None:
        """Save the UDF to a file and register it."""
        # Create directory if it doesn't exist
        os.makedirs("generated_udfs", exist_ok=True)
        
        # Save to file
        file_name = f"generated_udfs/{func_name}.py"
        with open(file_name, "w") as f:
            f.write(python_code)
        
        # Execute the code to get the function object
        namespace = {}
        exec(python_code, namespace)
        self.udfs[func_name] = (python_code, namespace[func_name])
        
        # Register with SQLite if table is available
        if self.table is not None:
            self.table.register_udf(func_name, len(self._get_function_params(python_code)), 
                                  namespace[func_name])

    def _get_function_params(self, python_code: str) -> List[str]:
        """Extract parameter names from function definition."""
        tree = ast.parse(python_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                return [arg.arg for arg in node.args.args]
        return []

    def load_udf(self, func_name: str) -> Optional[Callable]:
        """Load a previously saved UDF."""
        file_name = f"generated_udfs/{func_name}.py"
        if not os.path.exists(file_name):
            return None
            
        with open(file_name, "r") as f:
            python_code = f.read()
            
        # Execute the code to get the function object
        namespace = {}
        exec(python_code, namespace)
        
        # Save to dictionary and optionally register with SQLite
        self.udfs[func_name] = (python_code, namespace[func_name])
        if self.table is not None:
            self.table.register_udf(func_name, len(self._get_function_params(python_code)), 
                                  namespace[func_name])
        
        return namespace[func_name]

    def get_udf_code(self, func_name: str) -> Optional[str]:
        """Get the source code of a saved UDF."""
        if func_name in self.udfs:
            return self.udfs[func_name][0]
        return None

    def list_udfs(self) -> List[str]:
        """List all available UDFs."""
        return list(self.udfs.keys())

    def delete_udf(self, func_name: str) -> bool:
        """Delete a UDF from both memory and disk."""
        file_name = f"generated_udfs/{func_name}.py"
        if os.path.exists(file_name):
            os.remove(file_name)
            
        if func_name in self.udfs:
            del self.udfs[func_name]
            return True
        return False 