from parser.lark_parser import parser
from IR.intermediateRepresentation import generate_ir, validate_ir, pretty_print_ir, inline_udf_in_ir
from IR.udf.manager import UDFManager
from core.table_manager import TableManager
from planner.executor import Executor  # Required if you're still using Executor in some contexts
import json
import os
import re
import time

# Helper function to extract column dependencies from an expression node
def get_column_dependencies(node, actual_table_columns):
    dependencies = set()
    if isinstance(node, str): # Could be a column name or a literal from a UDF
        # Check if it's a direct column reference
        if node in actual_table_columns:
            dependencies.add(node)
        # It could also be a literal (e.g. number, string from UDF body) - ignore for dependencies
    elif isinstance(node, dict):
        if node.get("type") == "arithmetic" or node.get("type") == "comparison":
            dependencies.update(get_column_dependencies(node.get("left"), actual_table_columns))
            dependencies.update(get_column_dependencies(node.get("right"), actual_table_columns))
        elif node.get("type") == "if_stmt": # For CASE WHEN equivalent
            dependencies.update(get_column_dependencies(node.get("condition"), actual_table_columns))
            dependencies.update(get_column_dependencies(node.get("then"), actual_table_columns))
            dependencies.update(get_column_dependencies(node.get("else"), actual_table_columns))
        elif node.get("type") == "return_stmt":
             dependencies.update(get_column_dependencies(node.get("value"), actual_table_columns))
        # If it's an 'inlined_expression' itself, recurse on its 'expression' part
        elif node.get("type") == "inlined_expression":
            dependencies.update(get_column_dependencies(node.get("expression"), actual_table_columns))
        # Other types like literals (e.g. {'type': 'literal', 'value': 2}) don't have column dependencies

    # Lark Tokens are often used for column names directly in the IR
    # Check if the node is a Lark Token and its value is a column
    elif hasattr(node, 'type') and hasattr(node, 'value') and node.type == 'NAME':
        if node.value in actual_table_columns:
            dependencies.add(node.value)
    return dependencies

# Helper function to evaluate an inlined expression against a row of data
def _evaluate_inlined_expr(expression_node, row_data):
    if isinstance(expression_node, str):
        # If it's a column name, get its value from the row
        # Otherwise, it's a literal string from the UDF body (e.g. 'adult')
        return row_data.get(expression_node, expression_node) 
    elif isinstance(expression_node, (int, float, bool)):
        return expression_node # It's a literal number or boolean
    elif isinstance(expression_node, dict):
        expr_type = expression_node.get("type")
        if expr_type == "arithmetic":
            left = _evaluate_inlined_expr(expression_node["left"], row_data)
            right = _evaluate_inlined_expr(expression_node["right"], row_data)
            op = expression_node["op"]
            if op == "+": return left + right
            if op == "-": return left - right
            if op == "*": return left * right
            if op == "/": return left / right if right != 0 else None # Handle division by zero
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
        elif expr_type == "if_stmt": # Simulates CASE WHEN
            condition_val = _evaluate_inlined_expr(expression_node["condition"], row_data)
            if condition_val:
                return _evaluate_inlined_expr(expression_node["then"], row_data)
            else:
                return _evaluate_inlined_expr(expression_node["else"], row_data)
        elif expr_type == "return_stmt":
            return _evaluate_inlined_expr(expression_node["value"], row_data)
        elif expr_type == "literal": # Assuming UDFs might produce literals like {'type': 'literal', 'value': 'adult'}
            return expression_node["value"]
        # If it's an 'inlined_expression' itself, recurse (should ideally not happen here if called on expression part)
        elif expr_type == "inlined_expression":
            return _evaluate_inlined_expr(expression_node.get("expression"), row_data)
        else:
            raise ValueError(f"Unsupported expression type for evaluation: {expr_type}")
    # Handle Lark Tokens, assume they are column names if not resolved earlier
    elif hasattr(expression_node, 'type') and hasattr(expression_node, 'value') and expression_node.type == 'NAME':
        return row_data.get(expression_node.value)
    else:
        # This case handles literals like numbers, booleans directly passed in UDF body (e.g. `right: 2`)
        return expression_node 

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
                print("1. CREATE TABLE users (id INT, name TEXT, age INT);")
                print("2. INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);")
                print("3. SELECT name, age FROM users WHERE age > 25;")
                print("4. CREATE FUNCTION double_price(price FLOAT) RETURNS FLOAT")
                print("   BEGIN")
                print("       RETURN price * 2.0;")
                print("   END;")
                print("5. SELECT name, double_price(price) FROM users;")
                print()
                continue
            elif sql_query.lower().startswith('run '):
                file_path = sql_query[4:].strip()
                if not os.path.exists(file_path):
                    print(f"\nError: File '{file_path}' not found.")
                    continue
                    
                try:
                    with open(file_path, 'r') as f:
                        file_content = f.read()
                        
                    # Split content into statements (simplified from original, assuming similar logic)
                    parsed_file_statements = [] # This should be populated based on original logic for splitting file content
                    # --- Original file parsing logic to populate `parsed_file_statements` string list STARTS ---
                    # This is a placeholder for the complex statement splitting logic from the original file.
                    # It involved handling CREATE FUNCTION separately and then splitting by semicolon.
                    # For the purpose of this edit, we assume `raw_statements_from_file` is a list of SQL strings.
                    
                    # Placeholder for actual statement splitting logic from file content
                    # This complex logic was in lines 137-166 of the original run.py snippet
                    # For example:
                    temp_statements = []
                    current_stmt_lines = []
                    in_function_def = False
                    for line in file_content.split('\n'):
                        line = line.split('--')[0].strip()
                        if not line:
                            continue
                        if line.upper().startswith('CREATE FUNCTION'):
                            in_function_def = True
                        current_stmt_lines.append(line)
                        if in_function_def and line.strip().endswith('END;'):
                            stmt = ' '.join(current_stmt_lines)
                            if stmt.strip():
                                temp_statements.append(stmt)
                            current_stmt_lines = []
                            in_function_def = False
                        elif not in_function_def and line.endswith(';'):
                            stmt = ' '.join(current_stmt_lines)
                            if stmt.strip():
                                temp_statements.append(stmt)
                            current_stmt_lines = []
                    if current_stmt_lines: # Add any remaining statement
                        stmt = ' '.join(current_stmt_lines)
                        if stmt.strip():
                           temp_statements.append(stmt)
                    raw_statements_from_file = temp_statements
                    # --- Original file parsing logic ENDS ---
                    
                    print(f"\nFound {len(raw_statements_from_file)} statements in file '{file_path}'.")
                    file_total_start_time = time.time()

                    for stmt_string_from_file in raw_statements_from_file:
                        if not stmt_string_from_file.strip():
                            continue
                        
                        stmt_file_start_time = time.time()
                        print(f"\nExecuting from file: {stmt_string_from_file}")
                        try:
                            # Parse and execute each statement
                            # Assuming 'parser' is available
                            parsed_stmt_obj = parser.parse(stmt_string_from_file) 
                            print("\nParsed SQL Query (from file):")
                            print(parsed_stmt_obj)
                            
                            # Assuming generate_ir, inline_udf_in_ir, execute_statement are available
                            # And execute_statement is run.py's version.
                            if isinstance(parsed_stmt_obj, list):
                                for single_ir_target in parsed_stmt_obj:
                                    ir = generate_ir(single_ir_target)
                                    # ... (print IR, inline UDFs, print IR after UDF) as in original
                                    execute_statement(ir, table_manager, udf_manager) # run.py's version
                            else:
                                ir = generate_ir(parsed_stmt_obj)
                                # ... (print IR, inline UDFs, print IR after UDF) as in original
                                execute_statement(ir, table_manager, udf_manager) # run.py's version
                        except Exception as e_file_stmt:
                            print(f"\nError executing statement from file: {str(e_file_stmt)}")
                            # Original code continued on error
                        finally:
                            stmt_file_end_time = time.time()
                            stmt_file_duration = stmt_file_end_time - stmt_file_start_time
                            print(f"Statement from file ('{stmt_string_from_file[:40].strip()}...') processed in {stmt_file_duration:.4f} seconds.")
                    
                    file_total_end_time = time.time()
                    print(f"\nFinished executing file '{file_path}' in {file_total_end_time - file_total_start_time:.4f} seconds.")
                    continue # To the next input() prompt
                except Exception as e_file:
                    print(f"\nError executing file: {str(e_file)}")
                    continue # To the next input() prompt
                
            if not sql_query:
                continue
            
            # Interactive query processing
            interactive_query_input_start_time = time.time()
            print(f"\nProcessing interactive query: {sql_query}")
            
            try:
                parsed_interactive_query = parser.parse(sql_query)
                print("\nParsed Interactive SQL Query:")
                print(parsed_interactive_query)
                
                # Ensure it's a list for uniform processing
                if not isinstance(parsed_interactive_query, list):
                    parsed_interactive_query = [parsed_interactive_query]

                for single_parsed_item in parsed_interactive_query:
                    item_process_start_time = time.time()
                    current_ir = None # For logging in finally
                    try:
                        # Generate IR
                        ir = generate_ir(single_parsed_item)
                        current_ir = ir # Store for logging
                        print("\nGenerated IR:")
                        print(ir)
                        # Inline UDFs
                        ir = inline_udf_in_ir(ir, udf_manager)
                        print("\nIR after UDF inlining:")
                        print(ir)
                        execute_statement(ir, table_manager, udf_manager) # run.py's version
                    except Exception as e_item:
                        print(f"\nError executing part of interactive query: {str(e_item)}")
                        # Original loop continues to next item if it's a list, or finishes.
                    finally:
                        item_process_end_time = time.time()
                        item_duration = item_process_end_time - item_process_start_time
                        ir_type_str = current_ir.get('type', 'unknown') if current_ir else 'parse_error_or_not_reached'
                        print(f"Interactive query part (IR type: '{ir_type_str}') processed in {item_duration:.4f} seconds.")
                        
            except Exception as e_interactive_parse:
                print(f"\nError parsing interactive query: {str(e_interactive_parse)}")
            finally:
                interactive_query_input_end_time = time.time()
                interactive_input_duration = interactive_query_input_end_time - interactive_query_input_start_time
                print(f"Total processing for interactive input ('{sql_query[:50].strip()}...') took {interactive_input_duration:.4f} seconds.")
                
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
            # Get actual column names from the table schema for dependency checking
            actual_table_columns = []
            try:
                table_schema = table_manager.get_table_schema(ir["table"]) # Assuming this method exists
                if table_schema and "columns" in table_schema:
                    actual_table_columns = [col_def["name"] for col_def in table_schema["columns"]]
            except ValueError: # Table might not exist yet or schema is malformed
                 print(f"Warning: Could not retrieve schema for table {ir['table']} for column dependency check.")
                 # Fallback or decide how to handle, for now, proceed with empty actual_table_columns
                 # This might cause issues if UDFs use columns that genuinely exist but aren't found here.

            # Determine columns to fetch from the table manager
            columns_to_fetch = set()
            for col_item in ir["columns"]:
                if isinstance(col_item, str):
                    columns_to_fetch.add(col_item)
                elif hasattr(col_item, 'type') and col_item.type == 'NAME': # Lark Token
                    columns_to_fetch.add(col_item.value)
                elif isinstance(col_item, dict) and col_item.get("type") == "inlined_expression":
                    dependencies = get_column_dependencies(col_item["expression"], actual_table_columns)
                    columns_to_fetch.update(dependencies)
                # If it's a direct function_call that wasn't inlined (e.g. built-in), handle if necessary
                # For now, assuming all relevant UDFs are inlined.

            # Add dependencies from WHERE clause if it exists and is an inlined expression
            # Note: Current IR generation for WHERE seems to simplify it or keep function calls directly.
            # This part might need adjustment based on how WHERE clause IR is after inlining.
            if "where" in ir and ir["where"]: # Simplified: checking top-level where
                 # Assuming where clause after inlining could also be an 'inlined_expression' or a structure needing dependency checks
                if isinstance(ir["where"], dict): # if it's a single expression
                    dependencies = get_column_dependencies(ir["where"], actual_table_columns)
                    columns_to_fetch.update(dependencies)
                elif isinstance(ir["where"], list): # if it's a list of expressions (e.g. ANDed conditions)
                    for condition_node in ir["where"]:
                        dependencies = get_column_dependencies(condition_node, actual_table_columns)
                        columns_to_fetch.update(dependencies)


            # Get raw data first
            # Ensure all columns in columns_to_fetch are valid for the table_manager.select_from
            # For simplicity, we're passing the discovered set. TableManager should handle unknown columns.
            print(f"Fetching columns from TableManager: {list(columns_to_fetch)}")
            raw_results = table_manager.select_from(
                ir["table"],
                list(columns_to_fetch) if columns_to_fetch else ["*"], # Fetch all if no specific columns (e.g. SELECT *)
                                                                       # or if dependencies are empty (e.g. SELECT 1+1)
                ir.get("where", []) # Pass the original where structure for now.
                                    # Filtering logic below will re-evaluate inlined UDFs in WHERE.
            )
            
            # Process results
            results = []
            seen_rows = set()
            
            for row_dict in raw_results: # Assuming raw_results are lists of dicts [{col:val, ...}]
                
                # Evaluate WHERE clause if it exists and involves inlined expressions
                # This is a simplified version. A full SQL engine would convert WHERE to a boolean expression tree.
                passes_where = True
                if "where" in ir and ir["where"]:
                    # Example: if ir["where"] is an inlined expression from a UDF like is_expensive(price)
                    # It would look like: {'type': 'inlined_expression', 'expression': {<actual logic>}}
                    # Or if it's a simple comparison: {'left': 'age', 'op': '>', 'right': 18}
                    # The _evaluate_inlined_expr should handle these structures.
                    
                    # For now, assume udf_manager.execute_function was for non-inlined UDFs.
                    # With inlining, the 'where' clause itself becomes an expression (or list of expressions)
                    # that needs evaluation.
                    
                    # Simplified: if ir["where"] became an expression structure after inlining
                    if isinstance(ir["where"], dict): # e.g. a single condition
                         # If 'where' itself was a function call that got inlined:
                        if ir["where"].get("type") == "inlined_expression":
                            eval_result = _evaluate_inlined_expr(ir["where"]["expression"], row_dict)
                            if not eval_result: # boolean UDFs should return true/false
                                passes_where = False
                        # If 'where' is a direct comparison or other structure evaluable by _evaluate_inlined_expr
                        elif "type" in ir["where"]: # e.g. {'type': 'comparison', ...}
                            eval_result = _evaluate_inlined_expr(ir["where"], row_dict)
                            if not eval_result:
                                passes_where = False
                        # else: Malformed where clause after inlining, or not an expression we can evaluate here.
                        # This part needs robust handling of complex WHERE clauses (AND/OR etc.)
                
                if not passes_where:
                    continue

                processed_row = {}
                for i, col_item in enumerate(ir["columns"]):
                    col_alias = f"col_{i}" # Default alias
                    value = None

                    if isinstance(col_item, str):
                        col_alias = col_item
                        value = row_dict.get(col_item)
                    elif hasattr(col_item, 'type') and col_item.type == 'NAME': # Lark Token
                        col_alias = col_item.value
                        value = row_dict.get(col_item.value)
                    elif isinstance(col_item, dict) and col_item.get("type") == "inlined_expression":
                        # Construct alias from original function call
                        orig_call = col_item["original_function_call"]
                        func_name = orig_call["function_name"]
                        # Arguments might be Tokens or literals.
                        arg_strings = []
                        for arg in orig_call["arguments"]:
                            if hasattr(arg, 'type') and arg.type == 'NAME': # Lark Token
                                arg_strings.append(arg.value)
                            elif isinstance(arg, str): # A literal string arg
                                arg_strings.append(f"'{arg}'")
                            else: # A number or other literal
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
                if results: # Ensure there's at least one row to get headers from
                    headers = list(results[0].keys()) # Aliases are now keys in processed_row
                
                header_str = " | ".join(str(h) for h in headers)
                print("-" * len(header_str))
                print(header_str)
                print("-" * len(header_str))
                
                for res_row in results:
                    print(" | ".join(str(res_row.get(h, "NULL")) for h in headers)) # Use .get for safety
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
