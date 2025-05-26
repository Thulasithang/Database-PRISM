import json
import os

# Directory containing individual JSON files like student.json, lecturer.json, etc.
DATA_FOLDER = "data"

# This will hold all the table data
tables = {}

# Load all .json files from the data folder
for filename in os.listdir(DATA_FOLDER):
    if filename.endswith(".json"):
        table_name = filename[:-5]  # Remove '.json'
        file_path = os.path.join(DATA_FOLDER, filename)
        with open(file_path, "r") as f:
            tables[table_name] = json.load(f)

