import unittest
from IR.intermediateRepresentation import generate_ir, validate_ir
from IR.udf.manager import UDFManager

class TestUDFIntegration(unittest.TestCase):
    def setUp(self):
        # Reset UDF manager state for each test
        self.udf_manager = UDFManager()
        
    def test_create_function_ir(self):
        # Test creating a UDF through the IR system
        parsed_query = {
            "type": "create_function",
            "function_definition": """
            CREATE FUNCTION test_func(x int) RETURNS int
            BEGIN
                RETURN x * 2;
            END;
            """
        }
        
        ir = generate_ir(parsed_query)
        self.assertEqual(ir["type"], "create_function")
        self.assertEqual(ir["function_name"], "test_func")
        self.assertEqual(ir["status"], "success")
        
    def test_udf_in_select(self):
        # First create a UDF
        create_query = {
            "type": "create_function",
            "function_definition": """
            CREATE FUNCTION double_value(x int) RETURNS int
            BEGIN
                RETURN x * 2;
            END;
            """
        }
        generate_ir(create_query)
        
        # Now use the UDF in a SELECT query
        select_query = {
            "type": "select",
            "columns": [
                {
                    "type": "function_call",
                    "function_name": "double_value",
                    "arguments": ["age"]
                },
                "name"
            ],
            "from": "users",
            "where": []
        }
        
        ir = generate_ir(select_query)
        self.assertEqual(ir["type"], "select")
        self.assertEqual(len(ir["columns"]), 2)
        self.assertEqual(ir["columns"][0]["type"], "udf_call")
        self.assertEqual(ir["columns"][0]["function"], "double_value")
        
    def test_invalid_function_creation(self):
        # Test creating a UDF with invalid syntax
        parsed_query = {
            "type": "create_function",
            "function_definition": """
            CREATE FUNCTION invalid(x string) RETURNS int  # string type not supported
            BEGIN
                RETURN x;
            END;
            """
        }
        
        with self.assertRaises(ValueError):
            generate_ir(parsed_query)
            
    def test_undefined_function_in_query(self):
        # Test using an undefined UDF in a query
        select_query = {
            "type": "select",
            "columns": [
                {
                    "type": "function_call",
                    "function_name": "nonexistent_func",
                    "arguments": ["value"]
                }
            ],
            "from": "data",
            "where": []
        }
        
        with self.assertRaises(ValueError):
            generate_ir(select_query)
            
    def test_udf_in_where_clause(self):
        # First create a UDF
        create_query = {
            "type": "create_function",
            "function_definition": """
            CREATE FUNCTION is_adult(age int) RETURNS int
            BEGIN
                RETURN age >= 18;
            END;
            """
        }
        generate_ir(create_query)
        
        # Use UDF in WHERE clause
        select_query = {
            "type": "select",
            "columns": ["name", "age"],
            "from": "users",
            "where": {
                "left": {
                    "type": "function_call",
                    "function_name": "is_adult",
                    "arguments": ["age"]
                },
                "op": "=",
                "right": 1
            }
        }
        
        ir = generate_ir(select_query)
        self.assertEqual(ir["type"], "select")
        self.assertTrue(any(f.get("column", {}).get("type") == "udf_call" 
                          for f in ir["filters"]))
                          
    def test_multiple_udfs_in_query(self):
        # Create two UDFs
        udfs = [
            """
            CREATE FUNCTION double_age(age int) RETURNS int
            BEGIN
                RETURN age * 2;
            END;
            """,
            """
            CREATE FUNCTION half_age(age int) RETURNS int
            BEGIN
                RETURN age / 2;
            END;
            """
        ]
        
        for udf in udfs:
            generate_ir({
                "type": "create_function",
                "function_definition": udf
            })
            
        # Use both UDFs in a query
        select_query = {
            "type": "select",
            "columns": [
                {
                    "type": "function_call",
                    "function_name": "double_age",
                    "arguments": ["age"]
                },
                {
                    "type": "function_call",
                    "function_name": "half_age",
                    "arguments": ["age"]
                }
            ],
            "from": "users",
            "where": []
        }
        
        ir = generate_ir(select_query)
        self.assertEqual(len(ir["columns"]), 2)
        self.assertTrue(all(col["type"] == "udf_call" for col in ir["columns"]))

if __name__ == '__main__':
    unittest.main() 