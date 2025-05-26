import json
import os
from typing import Dict, List, Any

class UDFStorage:
    """Handles persistence of UDF definitions to disk."""
    
    def __init__(self, storage_dir: str = "data/udfs"):
        """Initialize UDF storage with the specified directory."""
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        
    def save_udf(self, name: str, definition: str) -> None:
        """
        Save a UDF definition to disk.
        
        Args:
            name: Name of the UDF
            definition: Complete UDF definition text
        """
        filepath = os.path.join(self.storage_dir, f"{name}.udf")
        with open(filepath, "w") as f:
            json.dump({
                "name": name,
                "definition": definition
            }, f, indent=2)
            
    def load_all_udfs(self) -> List[str]:
        """
        Load all saved UDF definitions.
        
        Returns:
            List of UDF definition texts
        """
        definitions = []
        if not os.path.exists(self.storage_dir):
            return definitions
            
        for filename in os.listdir(self.storage_dir):
            if filename.endswith(".udf"):
                filepath = os.path.join(self.storage_dir, filename)
                with open(filepath, "r") as f:
                    data = json.load(f)
                    definitions.append(data["definition"])
                    
        return definitions
        
    def delete_udf(self, name: str) -> None:
        """
        Delete a UDF definition from disk.
        
        Args:
            name: Name of the UDF to delete
        """
        filepath = os.path.join(self.storage_dir, f"{name}.udf")
        if os.path.exists(filepath):
            os.remove(filepath) 