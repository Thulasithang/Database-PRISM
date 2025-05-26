import unittest
import os
import shutil
from IR.udf.manager import UDFManager
from IR.udf.storage import UDFStorage

class TestUDFPersistence(unittest.TestCase):
    def setUp(self):
        # Use a test directory for UDF storage
        self.test_storage_dir = "test_data/udfs"
        self.storage = UDFStorage(storage_dir=self.test_storage_dir)
        
    def tearDown(self):
        # Clean up test directory after each test
        if os.path.exists(self.test_storage_dir):
            shutil.rmtree(self.test_storage_dir)
            
    def test_save_and_load_udf(self):
        # Test saving a UDF definition
        udf_text = """
        CREATE FUNCTION test_func(x int) RETURNS int
        BEGIN
            RETURN x * 2;
        END;
        """
        
        # Save the UDF
        self.storage.save_udf("test_func", udf_text)
        
        # Verify the file exists
        self.assertTrue(os.path.exists(os.path.join(self.test_storage_dir, "test_func.udf")))
        
        # Load all UDFs and verify
        loaded_udfs = self.storage.load_all_udfs()
        self.assertEqual(len(loaded_udfs), 1)
        self.assertEqual(loaded_udfs[0].strip(), udf_text.strip())
        
    def test_delete_udf(self):
        # Save a UDF
        udf_text = """
        CREATE FUNCTION temp_func(x float) RETURNS float
        BEGIN
            RETURN x + 1.0;
        END;
        """
        self.storage.save_udf("temp_func", udf_text)
        
        # Verify it exists
        filepath = os.path.join(self.test_storage_dir, "temp_func.udf")
        self.assertTrue(os.path.exists(filepath))
        
        # Delete it
        self.storage.delete_udf("temp_func")
        
        # Verify it's gone
        self.assertFalse(os.path.exists(filepath))
        
    def test_udf_manager_persistence(self):
        # Create a UDF manager with test storage
        manager = UDFManager()
        manager.storage = UDFStorage(storage_dir=self.test_storage_dir)
        
        # Register a function
        udf_text = """
        CREATE FUNCTION persistent_func(a int, b int) RETURNS int
        BEGIN
            RETURN a + b;
        END;
        """
        name = manager.register_function(udf_text)
        
        # Verify it was saved to disk
        self.assertTrue(os.path.exists(os.path.join(self.test_storage_dir, f"{name}.udf")))
        
        # Create a new manager instance to test loading
        new_manager = UDFManager()
        new_manager.storage = UDFStorage(storage_dir=self.test_storage_dir)
        
        # Verify the function was loaded
        self.assertIn(name, new_manager.functions)
        
        # Test the loaded function
        func = new_manager.get_function(name)
        self.assertEqual(func(5, 3), 8)
        
    def test_multiple_udfs_persistence(self):
        manager = UDFManager()
        manager.storage = UDFStorage(storage_dir=self.test_storage_dir)
        
        # Create multiple UDFs
        udfs = [
            ("""
            CREATE FUNCTION func1(x int) RETURNS int
            BEGIN
                RETURN x * 2;
            END;
            """, "func1"),
            ("""
            CREATE FUNCTION func2(y float) RETURNS float
            BEGIN
                RETURN y / 2.0;
            END;
            """, "func2")
        ]
        
        # Register all UDFs
        for udf_text, name in udfs:
            registered_name = manager.register_function(udf_text)
            self.assertEqual(registered_name, name)
            
        # Create new manager and verify all functions are loaded
        new_manager = UDFManager()
        new_manager.storage = UDFStorage(storage_dir=self.test_storage_dir)
        
        for _, name in udfs:
            self.assertIn(name, new_manager.functions)
            
    def test_invalid_udf_loading(self):
        # Save an invalid UDF definition
        invalid_udf = """
        CREATE FUNCTION invalid(x bad_type) RETURNS int
        BEGIN
            RETURN x;
        END;
        """
        self.storage.save_udf("invalid", invalid_udf)
        
        # Create a new manager and verify it handles the error gracefully
        manager = UDFManager()
        manager.storage = UDFStorage(storage_dir=self.test_storage_dir)
        
        # The invalid UDF should not be loaded
        self.assertNotIn("invalid", manager.functions) 