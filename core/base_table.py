# core/base_table.py
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Callable, Optional, Union

class BaseTable(ABC):
    """
    Abstract base class defining the interface for table implementations.
    
    All concrete table implementations must inherit from this class and implement
    its abstract methods to ensure consistent behavior across different storage formats.
    """

    @abstractmethod
    def insert(self, values: List[Any]) -> None:
        """Insert a row into the table."""
        pass

    @abstractmethod
    def select_all(self) -> List[Dict[str, Any]]:
        """Return all rows in the table as a list of dictionaries."""
        pass

    @abstractmethod
    def save(self) -> None:
        """Persist current state of the table to storage."""
        pass

    @staticmethod
    @abstractmethod
    def load(name: str) -> "BaseTable":
        """Load an existing table from storage by name."""
        pass

    @staticmethod
    @abstractmethod
    def exists(name: str) -> bool:
        """Check if a table with the given name already exists."""
        pass