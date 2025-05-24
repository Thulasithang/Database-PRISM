def generate_ir(parsed_query):
    # Step 1: Extract relevant information from the parsed query
    type = parsed_query.get("type")
    table_name = parsed_query.get("from")
    columns = parsed_query.get("columns", [])
    filters = parsed_query.get("where", [])
    if filters:
        filters = [{"column": filters["left"], "operator": filters["op"], "value": filters["right"]}]
    else:
        filters = []

    # Step 2: Create the intermediate representation
    ir = {
        "type": type,
        "table": table_name,
        "columns": columns,
        "filters": filters
    }

    return ir

def validate_ir(ir, schema):
    # Step 1: Check if the table exists in the schema
    if ir["table"] not in schema:
        raise ValueError(f"Table {ir['table']} does not exist in the schema.")

    # Step 2: Validate columns
    for column in ir["columns"]:
        if column not in schema[ir["table"]]["columns"]:
            raise ValueError(f"Column {column} does not exist in table {ir['table']}.")

    # Step 3: Validate filters
    for filter_condition in ir["filters"]:
        print(filter_condition)
        if filter_condition["column"] not in schema[ir["table"]]["columns"]:
            raise ValueError(f"Filter column {filter_condition['column']} does not exist in table {ir['table']}.")

    return True

def pretty_print_ir(ir):
    # Step 1: Format the IR for better readability
    formatted_ir = f"Query on table: {ir['table']}\n"
    formatted_ir += "Columns: " + ", ".join(ir["columns"]) + "\n"
    formatted_ir += "Filters:\n"
    
    for filter_condition in ir["filters"]:
        formatted_ir += f"  - {filter_condition['column']} {filter_condition['operator']} {filter_condition['value']}\n"

    return formatted_ir


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


example_ir = generate_ir(parsed_query)
print("Generated IR: ", example_ir)
try:
    validate_ir(example_ir, schema)
    print("IR is valid.")
except ValueError as e:
    print("IR validation error: ", e)
print("Pretty Printed IR: ")
print(pretty_print_ir(example_ir))