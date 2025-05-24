from typing import Any, Callable, Dict, List, Optional
from functools import wraps

class UDFRegistry:
    """
    Registry for User Defined Functions in the database system.
    Manages registration and execution of custom functions.
    """
    
    def __init__(self):
        self._functions: Dict[str, Callable] = {}
        
    def register(self, name: Optional[str] = None) -> Callable:
        """
        Decorator to register a new user-defined function.
        
        Args:
            name: Optional custom name for the function. If not provided,
                 the function's actual name will be used.
        
        Returns:
            Decorator function that registers the UDF.
        
        Example:
            @udf_registry.register()
            def double(x):
                return x * 2
        """
        def decorator(func: Callable) -> Callable:
            func_name = name or func.__name__
            
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            
            self._functions[func_name] = wrapper
            return wrapper
        return decorator
    
    def get_function(self, name: str) -> Optional[Callable]:
        """
        Retrieve a registered function by name.
        
        Args:
            name: Name of the function to retrieve.
            
        Returns:
            The registered function or None if not found.
        """
        return self._functions.get(name)
    
    def list_functions(self) -> List[str]:
        """
        Get a list of all registered function names.
        
        Returns:
            List of registered function names.
        """
        return list(self._functions.keys())
    
    def unregister(self, name: str) -> bool:
        """
        Remove a function from the registry.
        
        Args:
            name: Name of the function to remove.
            
        Returns:
            True if function was removed, False if it didn't exist.
        """
        if name in self._functions:
            del self._functions[name]
            return True
        return False

# Global registry instance
udf_registry = UDFRegistry() 