from parser.lark_parser import parser
from IR.intermediateRepresentation import generate_ir, validate_ir, pretty_print_ir
from planner.executor import Executor
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

        executor = Executor(table_name=ir["table"], storage_type="json")
        if ir["type"] == "create_table":
            executor.create_table(ir["columns"])
        elif ir["type"] == "insert":
            executor.insert(ir["values"])
        elif ir["type"] == "select":
            results = executor.select(ir["criteria"])
            print("Query Results:")
            for row in results:
                print(row)

    except Exception as e:
        print("Error parsing SQL query:")
        print(e)
        # print("Please ensure your SQL syntax is correct.")
        # print("Example queries:")
        # print("1. SELECT name, age FROM users WHERE age >= 25 AND name != 'Alice'")
        # print("2. INSERT INTO users (id, name) VALUES (1, 'Bob')")
        # print("3. CREATE TABLE users (id INT, name TEXT)")