from parser.lark_parser import parser
from IR.intermediateRepresentation import generate_ir, validate_ir, pretty_print_ir
from IR.udf.manager import UDFManager
from core.table_manager import TableManager
import json
import os
import re

def main():
    # Initialize managers
    table_manager = TableManager()
    udf_manager = UDFManager()
    
    print("SQL Query Runner")
    print("Type 'exit' or 'quit' to exit")
    print("Type 'clear' to clear the screen")
    print("Type 'help' for example queries")
    print("Type 'run example_queries.sql' to run example queries")
    print()
    
    while True:
        try:
            print("Enter SQL query:")
            sql_query = input().strip()
            
            if sql_query.lower() in {'exit', 'quit'}:
                break
            elif sql_query.lower() == 'clear':
                os.system('clear' if os.name == 'posix' else 'cls')
                continue
            elif sql_query.lower() == 'help':
                print("\nExample queries:")
                print("1. CREATE TABLE users (id INT, name TEXT, age INT, price FLOAT);")
                print("2. INSERT INTO users (id, name, age, price) VALUES (1, 'Alice', 25, 100.0);")
                print("3. SELECT name, age FROM users WHERE age > 25;")
                print("4. CREATE FUNCTION double_price(price FLOAT) RETURNS FLOAT")
                print("   BEGIN")
                print("       RETURN price * 2.0;")
                print("   END;")
                print("5. SELECT name, double_price(price) FROM users;")
                print()
                continue
            elif sql_query.lower().startswith('run '):
                # Run SQL file
                file_path = sql_query[4:].strip()
                if not os.path.exists(file_path):
                    print(f"\nError: File '{file_path}' not found.")
                    continue
                    
                try:
                    with open(file_path, 'r') as f:
                        file_content = f.read()
                        
                    # First, find and process all function definitions
                    function_pattern = r'CREATE\s+FUNCTION\s+(\w+)\s*\((.*?)\)\s*RETURNS\s+(\w+)\s*BEGIN\s*(.*?)\s*END\s*;'
                    function_matches = re.finditer(function_pattern, file_content, re.IGNORECASE | re.MULTILINE | re.DOTALL)
                    
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
                            "name": name,
                            "params": params,
                            "return_type": return_type.upper(),
                            "body": body.strip()
                        }
                        
                        # Register function
                        try:
                            udf_manager.register_function(function_def)
                            print(f"Created function: {name}")
                        except Exception as e:
                            print(f"Error creating function {name}: {str(e)}")
                            
                    # Remove function definitions from content
                    file_content = re.sub(function_pattern, '', file_content)
                    
                    # Process remaining statements
                    statements = []
                    current_stmt = []
                    
                    # Split remaining content into statements
                    for line in file_content.split('\n'):
                        # Remove inline comments
                        line = line.split('--')[0].strip()
                        if not line:
                            continue
                            
                        current_stmt.append(line)
                        if line.endswith(';'):
                            statements.append(' '.join(current_stmt))
                            current_stmt = []
                            
                    # Execute each statement
                    for stmt in statements:
                        if not stmt.strip():
                            continue
                            
                        print(f"\nExecuting: {stmt}")
                        # Parse and execute each statement
                        result = parser.parse(stmt)
                        print("\nParsed SQL Query:")
                        print(result)
                        
                        # Generate IR
                        if isinstance(result, list):
                            for single_stmt in result:
                                ir = generate_ir(single_stmt)
                                print("\nGenerated IR:")
                                print(ir)
                                execute_statement(ir, table_manager, udf_manager)
                        else:
                            ir = generate_ir(result)
                            print("\nGenerated IR:")
                            print(ir)
                            execute_statement(ir, table_manager, udf_manager)
                        
                    print("\nFile execution completed.")
                    continue
                except Exception as e:
                    print(f"\nError executing file: {str(e)}")
                    continue
                
            if not sql_query:
                continue
                
            # Parse the query
            result = parser.parse(sql_query)
            print("\nParsed SQL Query:")
            print(result)
            
            # Generate IR
            if isinstance(result, list):
                for single_stmt in result:
                    ir = generate_ir(single_stmt)
                    print("\nGenerated IR:")
                    print(ir)
                    execute_statement(ir, table_manager, udf_manager)
            else:
                ir = generate_ir(result)
                print("\nGenerated IR:")
                print(ir)
                execute_statement(ir, table_manager, udf_manager)
                
        except Exception as e:
            print("\nError:")
            print(str(e))
            print()

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
                ir["table"],
                [col if isinstance(col, str) else col.get("arguments", [None])[0] for col in ir["columns"]],
                []  # We'll handle WHERE clause separately
            )
            
            # Process results with function calls
            results = []
            seen_rows = set()  # To prevent duplicates
            
            for row in raw_results:
                # First check WHERE clause if it exists
                if "filters" in ir and ir["filters"]:
                    filter_condition = ir["filters"][0]  # For now, handle only the first filter
                    if isinstance(filter_condition["column"], dict) and filter_condition["column"]["type"] == "function_call":
                        # Get function arguments from the row
                        args = []
                        for arg in filter_condition["column"]["arguments"]:
                            if isinstance(arg, str):
                                args.append(float(row[str(arg)]))  # Convert to float for numeric functions
                            else:
                                args.append(float(row[str(arg.value)]))  # Convert Token to string
                        # Call the function
                        result = udf_manager.execute_function(filter_condition["column"]["function_name"], args)
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
                                args.append(float(row[str(arg.value)]))  # Convert Token to string
                        # Call the function
                        result = udf_manager.execute_function(col["function_name"], args)
                        # Use a more descriptive column name
                        col_name = f"{col['function_name']}({','.join(str(arg.value) for arg in col['arguments'] if hasattr(arg, 'value'))})"
                        processed_row[col_name] = result
                    else:
                        processed_row[str(col.value) if hasattr(col, 'value') else str(col)] = row[str(col.value) if hasattr(col, 'value') else str(col)]
                
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
                        col_name = f"{col['function_name']}({','.join(str(arg.value) for arg in col['arguments'] if hasattr(arg, 'value'))})"
                        headers.append(col_name)
                    else:
                        headers.append(str(col.value) if hasattr(col, 'value') else str(col))
                
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

if __name__ == "__main__":
    main()