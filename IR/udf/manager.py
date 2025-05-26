import os
import json
from typing import Dict, Any, Callable
from .parser import UDFParser

class UDFManager:
    """Manages User-Defined Functions (UDFs)."""
    
    def __init__(self, data_dir: str = "data/udfs"):
        """Initialize the UDF manager with a data directory."""
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.udfs: Dict[str, Dict] = {}
        self.parser = UDFParser()
        self.functions: Dict[str, Callable] = {}
        self._load_saved_udfs()
        
    def _load_saved_udfs(self) -> None:
        """Load all saved UDFs from disk during initialization."""
        if os.path.exists(self.data_dir):
            for filename in os.listdir(self.data_dir):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.data_dir, filename)
                    try:
                        with open(filepath, 'r') as f:
                            udf_def = json.load(f)
                            self.register_function(udf_def, persist=False)
                    except Exception as e:
                        print(f"Warning: Failed to load UDF {filename}: {str(e)}")
        
    def register_function(self, udf_def: Dict[str, Any], persist: bool = True) -> str:
        """
        Register a new UDF.
        
        Args:
            udf_def: UDF definition containing name, params, return_type, and body
            persist: Whether to persist the UDF to disk (default: True)
            
        Returns:
            Name of the registered function
        """
        if "definition" in udf_def:
            # Parse the function definition
            parsed = self.parser.parse_function_definition(udf_def["definition"])
            name = parsed["name"]
            self.udfs[name] = parsed
        else:
            # Use the provided function definition directly
            name = udf_def["name"]
            self.udfs[name] = udf_def
            
        # Create the function object
        self.functions[name] = lambda *args: self.execute_function(name, args)
        
        # Save to disk if requested
        if persist:
            filepath = os.path.join(self.data_dir, f"{name}.json")
            with open(filepath, 'w') as f:
                json.dump(self.udfs[name], f, indent=2)
                
        return name
        
    def get_function(self, name: str) -> Dict[str, Any]:
        """Get a UDF by name."""
        if name not in self.udfs:
            raise ValueError(f"Function '{name}' not found")
        return self.udfs[name]
        
    def execute_function(self, name: str, args: list) -> Any:
        """Execute a UDF with given arguments."""
        if name not in self.udfs:
            raise ValueError(f"Function '{name}' not found")
            
        udf = self.udfs[name]
        
        # Validate argument count
        if len(args) != len(udf["params"]):
            raise ValueError(f"Function '{name}' expects {len(udf['params'])} arguments, got {len(args)}")
            
        # Create variable bindings
        bindings = {}
        for param, arg in zip(udf["params"], args):
            # Convert argument to appropriate type based on parameter type
            param_type = param["type"].lower()
            try:
                if param_type == "float":
                    arg_value = float(arg)
                elif param_type == "int":
                    arg_value = int(float(arg))  # Handle float strings that represent integers
                else:
                    arg_value = arg
                bindings[param["name"]] = arg_value
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid argument type for parameter '{param['name']}': expected {param_type}, got {type(arg).__name__}")
            
        # Execute the function body
        result = self._evaluate_expression(udf["body"], bindings)
        
        # Convert result to the declared return type
        return_type = udf["return_type"].lower()
        try:
            if return_type == "float":
                return float(result)
            elif return_type == "int":
                return int(float(result))
            elif return_type == "bool":
                return bool(result)
            else:
                return result
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid return value type: expected {return_type}, got {type(result).__name__}")
        
    def _evaluate_expression(self, expr: str, bindings: Dict[str, Any]) -> Any:
        """Evaluate an expression with given variable bindings."""
        # Replace variables with their values, longest names first to avoid partial matches
        for var_name, value in sorted(bindings.items(), key=lambda x: len(x[0]), reverse=True):
            # Convert value to string, handling special cases
            if isinstance(value, bool):
                value_str = str(value).lower()  # Convert True/False to 'true'/'false'
            else:
                value_str = str(value)
            expr = expr.replace(var_name, value_str)
            
        # Remove RETURN keyword and semicolons
        expr = expr.replace("RETURN", "").replace(";", "").strip()
        
        try:
            # Add proper boolean handling
            if expr.lower() in ('true', 'false'):
                return expr.lower() == 'true'
            
            # Evaluate the expression
            result = eval(expr)
            
            # Convert result to appropriate type
            if isinstance(result, bool):
                return result  # Return boolean as is
            elif isinstance(result, (int, float)):
                return float(result)  # Convert numeric results to float
            return result
        except Exception as e:
            raise ValueError(f"Error evaluating expression '{expr}': {str(e)}")
        
    def list_functions(self) -> Dict[str, Dict[str, Any]]:
        """List all registered functions."""
        return self.udfs
        
    def remove_function(self, name: str) -> None:
        """Remove a registered function and its persistent storage."""
        if name in self.functions:
            del self.functions[name]
            del self.udfs[name]
            filepath = os.path.join(self.data_dir, f"{name}.json")
            if os.path.exists(filepath):
                os.remove(filepath)
            
    def get_function_by_name(self, name: str) -> Callable:
        """
        Get a registered function by name.
        
        Args:
            name: Name of the function to get
            
        Returns:
            The callable function
            
        Raises:
            ValueError: If the function is not found
        """
        if name not in self.functions:
            raise ValueError(f"Function not found: {name}")
            
        return self.functions[name] 