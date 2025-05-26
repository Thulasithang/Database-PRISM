from parser.lark_parser import parser
from IR.intermediateRepresentation import generate_ir, validate_ir, pretty_print_ir
from IR.udf.manager import UDFManager
from core.table_manager import TableManager
import json
import os
import re

def execute_sql_command(sql_command: str, table_manager: TableManager, udf_manager: UDFManager) -> bool:
    """Execute a single SQL command and return the result."""
    try:
        # Split into individual statements
        statements = [stmt.strip() for stmt in sql_command.split(';') if stmt.strip()]
        print(f"\nFound {len(statements)} statements to execute")
        
        success = True
        for stmt in statements:
            print(f"\nExecuting statement: {stmt}")
            # Parse and execute the command
            result = parser.parse(stmt + ";")  # Add back the semicolon
            print(f"Parsed result: {result}")
            
            if isinstance(result, list):
                for single_stmt in result:
                    success &= execute_statement(single_stmt, table_manager, udf_manager)
            else:
                success &= execute_statement(result, table_manager, udf_manager)
                
        return success
            
    except Exception as e:
        print(f"Error executing command: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def execute_statement(ir: dict, table_manager: TableManager, udf_manager: UDFManager) -> bool:
    """Execute a single SQL statement."""
    try:
        print(f"\nExecuting statement of type: {ir['type']}")
        print("Statement IR: ", ir)
        
        # Handle different types of commands
        if ir["type"] == "create_table":
            # Create the table
            table_manager.create_table(ir["table"], ir["columns"])
            print(f"Created table: {ir['table']}")
            return True
            
        elif ir["type"] == "insert":
            # Insert data into table
            table_manager.insert_into(ir["table"], ir["columns"], ir["values"])
            print(f"Inserted data into table: {ir['table']}")
            return True
            
        elif ir["type"] == "select":
            # Get raw data first
            raw_results = table_manager.select_from(
                ir["from"],
                [col if isinstance(col, str) else col.get("arguments", [None])[0] for col in ir["columns"]],
                []  # We'll handle WHERE clause separately
            )
            
            # Process results with function calls
            results = []
            seen_rows = set()  # To prevent duplicates
            
            for row in raw_results:
                # First check WHERE clause if it exists
                if "where" in ir and ir["where"]:
                    where_clause = ir["where"]
                    if isinstance(where_clause, dict) and where_clause["type"] == "function_call":
                        # Get function arguments from the row
                        args = []
                        for arg in where_clause["arguments"]:
                            if isinstance(arg, str):
                                args.append(float(row[str(arg)]))  # Convert to float for numeric functions
                            else:
                                args.append(arg)
                        # Call the function
                        result = udf_manager.execute_function(where_clause["function_name"], args)
                        if not result:  # Skip this row if the WHERE condition is not met
                            continue
                
                # Process selected columns
                processed_row = {}
                for i, col in enumerate(ir["columns"]):
                    if isinstance(col, dict) and col["type"] == "function_call":
                        # Get function arguments from the row
                        args = []
                        for arg in col["arguments"]:
                            if isinstance(arg, str):
                                args.append(float(row[str(arg)]))  # Convert to float for numeric functions
                            else:
                                args.append(arg)
                        # Call the function
                        result = udf_manager.execute_function(col["function_name"], args)
                        # Use a more descriptive column name
                        col_name = f"{col['function_name']}({','.join(str(arg) for arg in col['arguments'] if isinstance(arg, str))})"
                        processed_row[col_name] = result
                    else:
                        processed_row[str(col)] = row[str(col)]
                
                # Convert row to a tuple for hashing (to prevent duplicates)
                row_tuple = tuple(sorted(processed_row.items()))
                if row_tuple not in seen_rows:
                    seen_rows.add(row_tuple)
                    results.append(processed_row)
            
            # Pretty print results
            if results:
                # Get column names in the correct order
                headers = []
                for col in ir["columns"]:
                    if isinstance(col, dict) and col["type"] == "function_call":
                        # Use argument names in the column name
                        col_name = f"{col['function_name']}({','.join(str(arg) for arg in col['arguments'] if isinstance(arg, str))})"
                        headers.append(col_name)
                    else:
                        headers.append(str(col))
                
                # Print header
                header_str = " | ".join(str(h) for h in headers)
                print("-" * len(header_str))
                print(header_str)
                print("-" * len(header_str))
                
                # Print rows
                for row in results:
                    print(" | ".join(str(row[h]) for h in headers))
                print("-" * len(header_str))
            else:
                print("No results found.")
            return True
            
        elif ir["type"] == "create_function":
            # Create function definition
            function_def = {
                "name": ir["name"],
                "params": ir["params"],
                "return_type": ir["return_type"],
                "body": ir["body"]
            }
            # Register the function with the UDF manager
            function_name = udf_manager.register_function(function_def)
            print(f"Created function: {function_name}")
            return True
            
        else:
            raise ValueError(f"Unsupported command type: {ir['type']}")
            
    except Exception as e:
        print(f"Error executing statement: {str(e)}")
        return False

def run_sql_file(file_path: str) -> bool:
    """
    Execute SQL commands from a file.
    
    Args:
        file_path: Path to the SQL file
    """
    # Check if file exists
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return False
        
    try:
        # Initialize managers
        table_manager = TableManager()
        udf_manager = UDFManager()
        
        # Read and process the SQL file
        with open(file_path, 'r') as file:
            content = file.read()
            
        # First, find and process all function definitions
        function_pattern = r'CREATE\s+FUNCTION\s+(\w+)\s*\((.*?)\)\s*RETURNS\s+(\w+)\s*BEGIN\s*(.*?)\s*END\s*;'
        function_matches = re.finditer(function_pattern, content, re.IGNORECASE | re.MULTILINE | re.DOTALL)
        
        # Process function definitions
        for match in function_matches:
            name = match.group(1)
            params_str = match.group(2)
            return_type = match.group(3)
            body = match.group(4)
            
            # Parse parameters
            params = []
            if params_str.strip():
                for param in params_str.split(','):
                    param_name, param_type = param.strip().split()
                    params.append({"name": param_name, "type": param_type.upper()})
                    
            # Create function definition
            function_def = {
                "definition": f"""
                CREATE FUNCTION {name}({', '.join(f'{p["name"]} {p["type"]}' for p in params)}) RETURNS {return_type.upper()}
                BEGIN
                    {body.strip()}
                END;
                """
            }
            
            # Register function
            try:
                udf_manager.register_function(function_def)
                print(f"Created function: {name}")
            except Exception as e:
                print(f"Error creating function {name}: {str(e)}")
                
        # Remove function definitions from content
        content = re.sub(function_pattern, '', content)
        
        # Process remaining statements
        statements = []
        current_stmt = []
        
        # Split remaining content into statements
        for line in content.split('\n'):
            # Remove inline comments
            line = line.split('--')[0].strip()
            if not line:
                continue
                
            current_stmt.append(line)
            if line.endswith(';'):
                statements.append(' '.join(current_stmt))
                current_stmt = []
                
        # Execute each statement
        success = True
        for stmt in statements:
            if stmt.strip():
                success &= execute_sql_command(stmt, table_manager, udf_manager)
                
        return success
        
    except Exception as e:
        print(f"Error processing SQL file: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python sql_runner.py <sql_file>")
        print("Example: python sql_runner.py queries.sql")
        sys.exit(1)
        
    sql_file = sys.argv[1]
    success = run_sql_file(sql_file)
    sys.exit(0 if success else 1) 