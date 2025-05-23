# tests/test_json_table.py
from core.json_table import JSONTable


def test_json_table():
    """
    Test suite for JSONTable implementation.

    This test verifies:
    - Table initialization
    - Row insertion
    - Data serialization to disk (save)
    - File existence check
    - Data deserialization from disk (load)
    - Consistency of stored and loaded data

    Ensures JSONTable adheres to BaseTable interface and behaves correctly.
    """
    table_name = "students"
    base_path = "data"

    # Initialize table and insert sample data
    print("Initializing JSONTable with columns ['id', 'name']")
    table = JSONTable(table_name, ["id", "name"])
    table.insert([1, "Alice"])
    table.insert([2, "Bob"])

    # Save table to disk
    print(f"Saving table to directory: {base_path}")
    table.save(base_path)

    # Verify that the file exists
    print(f"Checking if table file '{table_name}.json' exists in '{base_path}'")
    assert JSONTable.exists(table_name, base_path), f"Expected file {table_name}.json to exist in {base_path}"

    # Load table from disk
    print(f"Loading table '{table_name}' from '{base_path}'")
    loaded = JSONTable.load(table_name, base_path)

    # Verify data integrity after loading
    print("Validating consistency of loaded data")
    expected_data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
    assert loaded.select_all() == expected_data, "Loaded data does not match expected values"

    print("JSON table test passed successfully.")


if __name__ == "__main__":
    test_json_table()