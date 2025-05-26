import unittest
from IR.udf.parser import UDFParser
from IR.udf.compiler import UDFCompiler
from IR.udf.manager import UDFManager

class TestUDFParser(unittest.TestCase):
    def setUp(self):
        self.parser = UDFParser()

    def test_parse_simple_function(self):
        udf_text = """
        CREATE FUNCTION add_numbers(a int, b int) RETURNS int
        BEGIN
            RETURN a + b;
        END;
        """
        result = self.parser.parse_function_definition(udf_text)
        self.assertEqual(result['name'], 'add_numbers')
        self.assertEqual(len(result['arguments']), 2)
        self.assertEqual(result['return_type'], 'int')
        self.assertTrue('a + b' in result['body'])

    def test_parse_float_function(self):
        udf_text = """
        CREATE FUNCTION average(x float, y float) RETURNS float
        BEGIN
            RETURN (x + y) / 2;
        END;
        """
        result = self.parser.parse_function_definition(udf_text)
        self.assertEqual(result['name'], 'average')
        self.assertEqual(result['return_type'], 'float')
        self.assertEqual([arg['type'] for arg in result['arguments']], ['float', 'float'])

    def test_invalid_syntax(self):
        invalid_texts = [
            # Missing RETURNS keyword
            """
            CREATE FUNCTION bad(x int) int
            BEGIN
                RETURN x;
            END;
            """,
            # Missing BEGIN/END
            """
            CREATE FUNCTION bad(x int) RETURNS int
                RETURN x;
            """,
            # Invalid argument format
            """
            CREATE FUNCTION bad(x invalid_type) RETURNS int
            BEGIN
                RETURN x;
            END;
            """
        ]
        for text in invalid_texts:
            with self.assertRaises(ValueError):
                self.parser.parse_function_definition(text)

    def test_parse_body(self):
        body = """
            x = a + b;
            y = x * 2;
            RETURN y;
        """
        operations = self.parser.parse_body(body)
        self.assertEqual(len(operations), 3)
        self.assertEqual(operations[0]['type'], 'assignment')
        self.assertEqual(operations[2]['type'], 'return')

class TestUDFCompiler(unittest.TestCase):
    def setUp(self):
        self.compiler = UDFCompiler()

    def test_compile_int_function(self):
        # Test compiling a simple integer function
        func = self.compiler.compile_function(
            name='test_int',
            arg_types=['int', 'int'],
            return_type='int',
            body='return a + b'
        )
        self.assertIsNotNone(func)

    def test_compile_float_function(self):
        # Test compiling a simple float function
        func = self.compiler.compile_function(
            name='test_float',
            arg_types=['float'],
            return_type='float',
            body='return x * 2.0'
        )
        self.assertIsNotNone(func)

    def test_invalid_type(self):
        with self.assertRaises(ValueError):
            self.compiler.compile_function(
                name='invalid_type',
                arg_types=['string'],  # Unsupported type
                return_type='int',
                body='return 0'
            )

class TestUDFManager(unittest.TestCase):
    def setUp(self):
        self.manager = UDFManager()

    def test_register_and_execute(self):
        udf_text = """
        CREATE FUNCTION test_add(a int, b int) RETURNS int
        BEGIN
            RETURN a + b;
        END;
        """
        name = self.manager.register_function(udf_text)
        self.assertEqual(name, 'test_add')
        self.assertIn(name, self.manager.functions)

    def test_remove_function(self):
        udf_text = """
        CREATE FUNCTION temp_func(x float) RETURNS float
        BEGIN
            RETURN x * 2;
        END;
        """
        name = self.manager.register_function(udf_text)
        self.assertIn(name, self.manager.functions)
        
        self.manager.remove_function(name)
        self.assertNotIn(name, self.manager.functions)

    def test_list_functions(self):
        udf_text1 = """
        CREATE FUNCTION func1(a int) RETURNS int
        BEGIN
            RETURN a * 2;
        END;
        """
        udf_text2 = """
        CREATE FUNCTION func2(x float) RETURNS float
        BEGIN
            RETURN x + 1.0;
        END;
        """
        self.manager.register_function(udf_text1)
        self.manager.register_function(udf_text2)
        
        functions = self.manager.list_functions()
        self.assertEqual(len(functions), 2)
        self.assertIn('func1', functions)
        self.assertIn('func2', functions)

    def test_duplicate_function(self):
        udf_text = """
        CREATE FUNCTION duplicate(x int) RETURNS int
        BEGIN
            RETURN x;
        END;
        """
        self.manager.register_function(udf_text)
        # Registering the same function again should work (overwrite)
        name = self.manager.register_function(udf_text)
        self.assertEqual(name, 'duplicate')

if __name__ == '__main__':
    unittest.main() 