import unittest
import ctypes
from IR.udf.compiler import UDFCompiler
from IR.udf.manager import UDFManager

class TestUDFExecution(unittest.TestCase):
    def setUp(self):
        self.compiler = UDFCompiler()
        self.manager = UDFManager()

    def test_execute_int_addition(self):
        # Test a simple integer addition function
        udf_text = """
        CREATE FUNCTION add_ints(a int, b int) RETURNS int
        BEGIN
            RETURN a + b;
        END;
        """
        
        name = self.manager.register_function(udf_text)
        func = self.manager.get_function(name)
        
        # Test with different integer inputs
        test_cases = [
            (5, 3, 8),
            (0, 0, 0),
            (-1, 1, 0),
            (100, 200, 300)
        ]
        
        for a, b, expected in test_cases:
            result = func(a, b)
            self.assertEqual(result, expected)

    def test_execute_float_operations(self):
        # Test floating point operations
        udf_text = """
        CREATE FUNCTION float_ops(x float) RETURNS float
        BEGIN
            RETURN x * 2.5;
        END;
        """
        
        name = self.manager.register_function(udf_text)
        func = self.manager.get_function(name)
        
        # Test with different float inputs
        test_cases = [
            (1.0, 2.5),
            (0.0, 0.0),
            (-1.0, -2.5),
            (2.5, 6.25)
        ]
        
        for x, expected in test_cases:
            result = func(ctypes.c_float(x).value)
            self.assertAlmostEqual(result, expected, places=5)

    def test_execute_complex_function(self):
        # Test a more complex function with multiple operations
        udf_text = """
        CREATE FUNCTION complex_calc(a int, b int, c float) RETURNS float
        BEGIN
            x = a + b;
            y = x * c;
            RETURN y / 2.0;
        END;
        """
        
        name = self.manager.register_function(udf_text)
        func = self.manager.get_function(name)
        
        # Test cases with mixed integer and float operations
        test_cases = [
            (1, 2, 1.5, 2.25),  # (1 + 2) * 1.5 / 2.0 = 2.25
            (0, 0, 1.0, 0.0),   # (0 + 0) * 1.0 / 2.0 = 0.0
            (10, -5, 2.0, 5.0)  # (10 + -5) * 2.0 / 2.0 = 5.0
        ]
        
        for a, b, c, expected in test_cases:
            result = func(a, b, ctypes.c_float(c).value)
            self.assertAlmostEqual(result, expected, places=5)

    def test_function_error_handling(self):
        # Test error handling in function execution
        with self.assertRaises(ValueError):
            # Try to execute a function with wrong number of arguments
            udf_text = """
            CREATE FUNCTION single_arg(x int) RETURNS int
            BEGIN
                RETURN x;
            END;
            """
            name = self.manager.register_function(udf_text)
            func = self.manager.get_function(name)
            func(1, 2)  # Should raise error - too many arguments

    def test_type_conversion(self):
        # Test automatic type conversion between Python and C types
        udf_text = """
        CREATE FUNCTION mixed_types(i int, f float) RETURNS float
        BEGIN
            RETURN i * f;
        END;
        """
        
        name = self.manager.register_function(udf_text)
        func = self.manager.get_function(name)
        
        # Test with different type combinations
        result = func(2, ctypes.c_float(1.5).value)
        self.assertAlmostEqual(result, 3.0, places=5)
        
        result = func(-1, ctypes.c_float(2.5).value)
        self.assertAlmostEqual(result, -2.5, places=5)

if __name__ == '__main__':
    unittest.main() 