from parser.lark_parser import parser
from IR.intermediateRepresentation import generate_ir, validate_ir, pretty_print_ir
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
    except Exception as e:
        print("Error parsing SQL query:")
        print(e)
        # print("Please ensure your SQL syntax is correct.")
        # print("Example queries:")
        # print("1. SELECT name, age FROM users WHERE age >= 25 AND name != 'Alice'")
        # print("2. INSERT INTO users (name, age) VALUES ('Bob', 30)")
        # print("3. CREATE TABLE users (id INT, name TEXT)")