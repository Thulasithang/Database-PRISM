from parser.lark_parser import parser
from IR.intermediateRepresentation import generate_ir, validate_ir, pretty_print_ir
from planner.executor import Executor
from executor.execution_engine import execute_query
import json


with open("data/students.json", "r") as file:
    schema = json.load(file)

print(schema)

while True:
    print("print the sql query: ")
    sql_query = input()
    try:
        result = parser.parse(sql_query)
        print("Parsed SQL Query:")
        print(result)
        ir = generate_ir(result)
        print("Generated Intermediate Representation (IR):")
        print(ir)
        validate_ir(ir, schema=schema)
        print("IR validation passed.")
        pretty_ir = pretty_print_ir(ir) if ir["type"] == "select" else ""
        print("Pretty Printed IR:")
        print(pretty_ir)

        executor = execute_query(ir)
        query_type = ir["type"]

        if query_type in ("select", "show_table", "describe_table"):
            # Expected to return (schema, rows)
            schema, rows = result
            print("Schema:", schema)
            if rows:
                print("Rows:")
                for row in rows:
                    print(row)
            else:
                print("No rows found.")
        elif query_type in ("insert", "update", "delete", "create_table", "drop_table",
                            "rename_table"):
            # Other known query types returning dict or status messages
            if isinstance(result, dict):
                print("Execution Result:")
                for field_name, field_value in result.items():
                    print(f"{field_name}: {field_value}")
            else:
                print("Execution Result:", result)
        else:
            print("Unsupported IR type:", query_type)

    except Exception as e:
        print("Error parsing SQL query:")
        print(e)
        # print("Please ensure your SQL syntax is correct.")
        # print("Example queries:")
        # print("1. SELECT name, age FROM users WHERE age >= 25 AND name != 'Alice'")
        # print("2. INSERT INTO users (id, name) VALUES (1, 'Bob')")
        # print("3. CREATE TABLE users (id INT, name TEXT)")