from .udf.manager import UDFManager

# Initialize the UDF manager as a global instance
udf_manager = UDFManager()

def generate_ir(parsed_query):
    # Step 1: Extract relevant information from the parsed query
    print("Generating IR from parsed query: ", parsed_query)
    type = parsed_query.get("type")
    
    # Handle UDF creation
    if type == "create_function":
        # Extract function definition parts
        name = parsed_query.get("name")
        params = parsed_query.get("params", [])
        return_type = parsed_query.get("return_type")
        body = parsed_query.get("body")
        
        if not all([name, return_type, body]):
            raise ValueError("Missing required function definition parts")
            
        # Create function definition
        function_def = {
            "name": name,
            "params": params,
            "return_type": return_type,
            "body": body
        }
        
        return {
            "type": "create_function",
            "name": name,
            "params": params,
            "return_type": return_type,
            "body": body
        }
    
    # Handle regular queries
    condition = "from" if type == "select" else "table" if type == "insert" else "table" if type == "create_table" else None
    print("condition: ", condition)
    table_name = parsed_query.get(condition, None)
    if not table_name:
        # For INSERT statements, get table name from the 'into' field
        if type == "insert":
            table_name = parsed_query.get("into")
            if not table_name:
                raise ValueError("Table name not found in parsed query.")
    elif not table_name:
        raise ValueError("Table name not found in parsed query.")
        
    columns = parsed_query.get("columns", [])
    
    # Handle WHERE clause
    where = parsed_query.get("where", [])
    if where:
        if isinstance(where, dict):
            # Single filter condition
            if where.get("type") == "function_call":
                # Function call in WHERE clause
                return {
                    "type": type,
                    "table": table_name,
                    "columns": columns,
                    "where": where  # Keep the function call structure intact
                }
            else:
                return {
                    "type": type,
                    "table": table_name,
                    "columns": columns,
                    "where": where  # Keep the comparison structure intact
                }
        else:
            # Multiple filter conditions
            filters = []
            for filter_condition in where:
                if filter_condition.get("type") == "function_call":
                    # Function call in WHERE clause
                    filters.append(filter_condition)  # Keep the function call structure intact
                else:
                    filters.append({
                        "column": filter_condition["left"],
                        "operator": filter_condition["op"],
                        "value": filter_condition["right"]
                    })
            return {
                "type": type,
                "table": table_name,
                "columns": columns,
                "where": filters
            }
    
    # Create the intermediate representation
    ir = {
        "type": type,
        "table": table_name,
        "columns": columns,
        "where": []  # Empty where clause
    }
    
    # Add values for INSERT statements
    if type == "insert":
        ir["values"] = parsed_query.get("values", [])

    print("Generated IR: ", ir)
    return ir

def _replace_params(body, params, args):
    """Helper function to replace parameter placeholders with actual arguments."""
    if isinstance(body, str):
        for i, param in enumerate(params):
            if body == param["name"]:
                if isinstance(args[i], str):
                    return args[i].strip("'\"") 
                return args[i]
        return body
    elif isinstance(body, dict):
        new_body = {}
        for key, value in body.items():
            new_body[key] = _replace_params(value, params, args)
        return new_body
    elif isinstance(body, list):
        return [_replace_params(item, params, args) for item in body]
    else:
        return body

def inline_udf_in_ir(ir, udf_manager: UDFManager):
    """Recursively traverse the IR and inline UDFs."""
    if isinstance(ir, dict):
        if ir.get("type") == "function_call":
            function_name = ir.get("function_name")
            arguments = ir.get("arguments", [])
            
            # Get UDF definition
            udf_def = udf_manager.get_function(function_name)
            if not udf_def:
                raise ValueError(f"UDF '{function_name}' not found.")

            inlined_body = _replace_params(udf_def["body"], udf_def["params"], arguments)
            
            # Return the inlined expression directly
            return {
                "type": "inlined_expression",
                "original_function_call": { 
                    "function_name": function_name,
                    "arguments": arguments 
                },
                "expression": inlined_body
            }
            
        else:
            # Recursively process other parts of the IR
            new_ir = {}
            for key, value in ir.items():
                new_ir[key] = inline_udf_in_ir(value, udf_manager)
            return new_ir
            
    elif isinstance(ir, list):
        return [inline_udf_in_ir(item, udf_manager) for item in ir]
        
    else:
        # Base case: not a dict or list, or not a function call
        return ir

import os
import json
from core.json_table import JSONTable
def validate_ir(ir, schema):
    # Handle UDF validation
    if ir["type"] == "create_function":
        # UDF validation is handled during registration
        return True
        
    # Step 1: Check if the table exists in the schema
    print("Validating IR: ", ir)
    if ir["type"] == "select":
        return validate_select_ir(ir, schema)
    elif ir["type"] == "create_table":
        return validate_create_table_ir(ir, schema)
    elif ir["type"] == "insert":
        table = ir["table"]
        table_exists = JSONTable.exists(table, base_path="data")
        if not table_exists:
            raise ValueError(f"Table {table} does not exist in the schema.")
        


def pretty_print_ir(ir):
    # Step 1: Format the IR for better readability
    print("Pretty printing IR: ", ir)
    formatted_ir = f"Query on table: {ir['table']}\n"
    formatted_ir += "Columns: " + ", ".join(ir["columns"]) + "\n"
    formatted_ir += "Filters:\n"
    print("formatted_ir: ", formatted_ir)
    if  len(ir["filters"]) == 0:
        formatted_ir += "  - No filters applied\n"
    else:
        formatted_ir += "  - Applied filters:\n"
        for filter_condition in ir["filters"]:
            formatted_ir += f"  - {filter_condition['column']} {filter_condition['operator']} {filter_condition['value']}\n"

    return formatted_ir



def validate_select_ir(ir, schema):
    table = ir["table"]
    file_path = f"data/{table}.json"
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            schema = json.load(file)
            try:
                # Step 2: Validate columns
                if ir["columns"] != ["all"]:
                        for column in ir["columns"]:
                            if column not in schema["columns"]:
                                print(f"Column {column} does not exist in table {ir['table']}.")
                                raise ValueError(f"Column {column} does not exist in table {ir['table']}.")

                # Step 3: Validate filters
                for filter_condition in ir["filters"]:
                    print("filter_condition: ", filter_condition)
                    if len(filter_condition) > 0 and filter_condition["column"] not in schema["columns"]:
                        print(f"Filter column {filter_condition['column']} does not exist in table {ir['table']}.")
                        raise ValueError(f"Filter column {filter_condition['column']} does not exist in table {ir['table']}.")

                return True
            except KeyError as e:
                print(f"Key error in schema validation: {e}")
                raise ValueError(f"Invalid schema structure for table {ir['table']}.")
        
    else:
        print(f"Table {ir['table']} does not exist in the schema.")
        raise ValueError(f"Table {ir['table']} does not exist in the schema.")


def validate_create_table_ir(ir, schema):
    table = ir["table"]
    table_exists = JSONTable.exists(table, base_path="data")
    if table_exists:
        raise ValueError(f"Table {table} already exists in the schema.")
    else:
        # Create a new table schema
        columns = ir["columns"]
        if not columns:
            raise ValueError("No columns specified for the new table.")
        column_set = set()
        for column in columns:
            if column["name"] in column_set:
                raise ValueError(f"Duplicate column name: {column['name']}")
            else: 
                column_set.add(column["name"])
        print("True")
        return True
    
# Example usage:
parsed_query = {
    "type": "select",
    "columns": ["name"],
    "from": "users",
    "where": {
        "left": "age",
        "op": ">",
        "right": 30
    }
}

generated_ir = {
    "type": "select",
    "table": "users",
    "columns": ["name"],
    "filters": [{"column": "age", "op": ">", "value": 30}]
}

schema = {
    "users": {
        "columns": ["id", "name", "age"]
    }
}


# example_ir = generate_ir(parsed_query)
# print("Generated IR: ", example_ir)
# try:
#     validate_ir(example_ir, schema)
#     print("IR is valid.")
# except ValueError as e:
#     print("IR validation error: ", e)
# print("Pretty Printed IR: ")
# print(pretty_print_ir(example_ir))