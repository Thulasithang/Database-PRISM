# tests/test_sqlite_table.py
import sqlite3
from core.sqlite_table import SQLiteTable


def test_sqlite_table():
    """
    Test suite for SQLiteTable implementation.
    
    This test verifies:
    - Table creation
    - Row insertion
    - Data selection
    - Row update
    - Row deletion
    
    Ensures SQLiteTable adheres to BaseTable interface and behaves correctly.
    """
    table_name = "students_sqlite"
    print("Setting up test environment for SQLite table operations.")

    # Clean up any existing table to ensure a fresh state
    if SQLiteTable.exists(table_name):
        print(f"Dropping existing table '{table_name}'...")
        with sqlite3.connect("db.sqlite") as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    else:
        print(f"No existing table '{table_name}' found. Proceeding with creation.")

    # Create new table and insert sample data
    print(f"\nCreating table '{table_name}' with columns ['id', 'name']")
    table = SQLiteTable(table_name, ["id", "name"], {"id": "INTEGER", "name": "TEXT"})
    
    print("Inserting initial test records: (1, 'Alice'), (2, 'Bob')")
    table.insert([1, "Alice"])
    table.insert([2, "Bob"])

    # Load table from database and verify initial data
    print("\nLoading table from database...")
    loaded = SQLiteTable.load(table_name)

    print("Verifying initial data consistency...")
    assert loaded.select_all() == [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
    print("Initial data verification successful.")

    # Perform an update operation
    print("\nUpdating name for id=1 to 'Charlie'")
    loaded.update({"name": "Charlie"}, "id = ?", (1,))
    
    print("Validating updated data...")
    assert loaded.select_all() == [
        {"id": 1, "name": "Charlie"},
        {"id": 2, "name": "Bob"}
    ]
    print("Update operation verified successfully.")

    # Perform a delete operation
    print("\nDeleting record where id=2")
    loaded.delete("id = ?", (2,))
    
    print("Verifying final dataset after deletion...")
    assert loaded.select_all() == [
        {"id": 1, "name": "Charlie"}
    ]
    print("Deletion operation verified successfully.")

    print("\nAll SQLite table operations tested successfully.")


if __name__ == "__main__":
    test_sqlite_table()