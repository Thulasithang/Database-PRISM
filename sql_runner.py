from parser.lark_parser import parser
from IR.intermediateRepresentation import generate_ir, validate_ir, pretty_print_ir, inline_udf_in_ir
from IR.udf.manager import UDFManager
from core.table_manager import TableManager
import json
import os
import re
import time

# Helper function to extract column dependencies from an expression node
def get_column_dependencies(node, actual_table_columns):
    dependencies = set()
    if isinstance(node, str):
        if node in actual_table_columns:
            dependencies.add(node)
    elif isinstance(node, dict):
        if node.get("type") == "arithmetic" or node.get("type") == "comparison":
            dependencies.update(get_column_dependencies(node.get("left"), actual_table_columns))
            dependencies.update(get_column_dependencies(node.get("right"), actual_table_columns))
        elif node.get("type") == "if_stmt":
            dependencies.update(get_column_dependencies(node.get("condition"), actual_table_columns))
            dependencies.update(get_column_dependencies(node.get("then"), actual_table_columns))
            dependencies.update(get_column_dependencies(node.get("else"), actual_table_columns))
        elif node.get("type") == "return_stmt":
             dependencies.update(get_column_dependencies(node.get("value"), actual_table_columns))
        elif node.get("type") == "inlined_expression":
            dependencies.update(get_column_dependencies(node.get("expression"), actual_table_columns))
    elif hasattr(node, 'type') and hasattr(node, 'value') and node.type == 'NAME':
        if node.value in actual_table_columns:
            dependencies.add(node.value)
    return dependencies

# Helper function to evaluate an inlined expression against a row of data
def _evaluate_inlined_expr(expression_node, row_data):
    if isinstance(expression_node, str):
        return row_data.get(expression_node, expression_node)
    elif isinstance(expression_node, (int, float, bool)):
        return expression_node
    elif isinstance(expression_node, dict):
        expr_type = expression_node.get("type")
        if expr_type == "arithmetic":
            left = _evaluate_inlined_expr(expression_node["left"], row_data)
            right = _evaluate_inlined_expr(expression_node["right"], row_data)
            op = expression_node["op"]
            if op == "+": return left + right
            if op == "-": return left - right
            if op == "*": return left * right
            if op == "/": return left / right if right != 0 else None
            raise ValueError(f"Unknown arithmetic operator: {op}")
        elif expr_type == "comparison":
            left = _evaluate_inlined_expr(expression_node["left"], row_data)
            right = _evaluate_inlined_expr(expression_node["right"], row_data)
            op = expression_node["op"]
            if op == ">": return left > right
            if op == "<": return left < right
            if op == ">=": return left >= right
            if op == "<=": return left <= right
            if op == "=": return left == right
            if op == "!=": return left != right
            raise ValueError(f"Unknown comparison operator: {op}")
        elif expr_type == "if_stmt":
            condition_val = _evaluate_inlined_expr(expression_node["condition"], row_data)
            if condition_val:
                return _evaluate_inlined_expr(expression_node["then"], row_data)
            else:
                return _evaluate_inlined_expr(expression_node["else"], row_data)
        elif expr_type == "return_stmt":
            return _evaluate_inlined_expr(expression_node["value"], row_data)
        elif expr_type == "literal":
            return expression_node["value"]
        elif expr_type == "inlined_expression":
            return _evaluate_inlined_expr(expression_node.get("expression"), row_data)
        else:
            raise ValueError(f"Unsupported expression type for evaluation: {expr_type}")
    elif hasattr(expression_node, 'type') and hasattr(expression_node, 'value') and expression_node.type == 'NAME':
        return row_data.get(expression_node.value)
    else:
        return expression_node

def execute_sql_command(sql_command: str, table_manager: TableManager, udf_manager: UDFManager) -> bool:
    """Execute a single SQL command and return the result."""
    try:
        # Split into individual statements
        statements = [stmt.strip() for stmt in sql_command.split(';') if stmt.strip()]
        print(f"\nFound {len(statements)} statements to execute")
        
        success = True
        for stmt in statements:
            print(f"\nExecuting statement: {stmt}")
            start_time_stmt = time.time()
            # Parse and execute the command
            result = parser.parse(stmt + ";")  # Add back the semicolon
            print(f"Parsed result: {result}")
            
            current_stmt_success = True
            if isinstance(result, list):
                for single_stmt_parsed in result: # Renamed to avoid conflict
                    # Generate IR for each statement
                    ir = generate_ir(single_stmt_parsed) 
                    print(f"Generated IR: {ir}")
                    # Inline UDFs
                    ir = inline_udf_in_ir(ir, udf_manager) 
                    print(f"IR after UDF inlining: {ir}")
                    current_stmt_success &= execute_statement(ir, table_manager, udf_manager)
            else:
                # Generate IR for the single statement
                ir = generate_ir(result) 
                print(f"Generated IR: {ir}")
                # Inline UDFs
                ir = inline_udf_in_ir(ir, udf_manager) 
                print(f"IR after UDF inlining: {ir}")
                current_stmt_success = execute_statement(ir, table_manager, udf_manager)
            
            end_time_stmt = time.time()
            duration_stmt = end_time_stmt - start_time_stmt
            print(f"Statement executed in {duration_stmt:.4f} seconds.")
            success &= current_stmt_success
                
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
            # Get actual column names from the table schema for dependency checking
            actual_table_columns = []
            try:
                # Assuming table_manager has a way to get schema to list actual columns
                table_schema = table_manager.get_table_schema(ir["table"])
                if table_schema and "columns" in table_schema:
                    actual_table_columns = [col_def["name"] for col_def in table_schema["columns"]]
            except ValueError:
                 print(f"Warning: Could not retrieve schema for table {ir['table']} for column dependency check.")

            columns_to_fetch = set()
            for col_item in ir["columns"]:
                if isinstance(col_item, str):
                    columns_to_fetch.add(col_item)
                elif hasattr(col_item, 'type') and col_item.type == 'NAME': 
                    columns_to_fetch.add(col_item.value)
                elif isinstance(col_item, dict) and col_item.get("type") == "inlined_expression":
                    dependencies = get_column_dependencies(col_item["expression"], actual_table_columns)
                    columns_to_fetch.update(dependencies)

            if "where" in ir and ir["where"]:
                if isinstance(ir["where"], dict):
                    dependencies = get_column_dependencies(ir["where"], actual_table_columns)
                    columns_to_fetch.update(dependencies)
                elif isinstance(ir["where"], list):
                    for condition_node in ir["where"]:
                        dependencies = get_column_dependencies(condition_node, actual_table_columns)
                        columns_to_fetch.update(dependencies)
            
            print(f"Fetching columns from TableManager: {list(columns_to_fetch)}")
            raw_results = table_manager.select_from(
                ir["table"], # Corrected from ir["from"] which might be a Lark specific detail pre-IR generation
                list(columns_to_fetch) if columns_to_fetch else ["*"],
                ir.get("where", []) 
            )
            
            results = []
            seen_rows = set()
            
            for row_dict in raw_results: 
                passes_where = True
                if "where" in ir and ir["where"]:
                    if isinstance(ir["where"], dict):
                        if ir["where"].get("type") == "inlined_expression":
                            eval_result = _evaluate_inlined_expr(ir["where"]["expression"], row_dict)
                            if not eval_result:
                                passes_where = False
                        elif "type" in ir["where"]:
                            eval_result = _evaluate_inlined_expr(ir["where"], row_dict)
                            if not eval_result:
                                passes_where = False
                
                if not passes_where:
                    continue

                processed_row = {}
                for i, col_item in enumerate(ir["columns"]):
                    col_alias = f"col_{i}"
                    value = None
                    if isinstance(col_item, str):
                        col_alias = col_item
                        value = row_dict.get(col_item)
                    elif hasattr(col_item, 'type') and col_item.type == 'NAME':
                        col_alias = col_item.value
                        value = row_dict.get(col_item.value)
                    elif isinstance(col_item, dict) and col_item.get("type") == "inlined_expression":
                        orig_call = col_item["original_function_call"]
                        func_name = orig_call["function_name"]
                        arg_strings = []
                        for arg in orig_call["arguments"]:
                            if hasattr(arg, 'type') and arg.type == 'NAME':
                                arg_strings.append(arg.value)
                            elif isinstance(arg, str):
                                arg_strings.append(f"'{arg}'")
                            else:
                                arg_strings.append(str(arg))
                        col_alias = f"{func_name}({', '.join(arg_strings)})"
                        value = _evaluate_inlined_expr(col_item["expression"], row_dict)
                    
                    processed_row[col_alias] = value
                
                row_tuple = tuple(sorted(processed_row.items()))
                if row_tuple not in seen_rows:
                    seen_rows.add(row_tuple)
                    results.append(processed_row)
            
            if results:
                headers = []
                if results:
                    headers = list(results[0].keys())
                
                header_str = " | ".join(str(h) for h in headers)
                print("-" * len(header_str))
                print(header_str)
                print("-" * len(header_str))
                
                for res_row in results:
                    print(" | ".join(str(res_row.get(h, "NULL")) for h in headers))
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
        start_time = time.time() # Record start time
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
                
        end_time = time.time() # Record end time
        duration = end_time - start_time
        print(f"\nSQL file '{file_path}' executed in {duration:.4f} seconds.")
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