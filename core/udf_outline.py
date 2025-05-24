from typing import Any, Callable, Dict, List, Optional, Union
from core.udf import udf_registry
from functools import reduce
import operator

class UDFOutline:
    """
    A class for creating complex UDFs that can handle SQL-like operations
    and common query patterns.
    """
    
    @staticmethod
    def aggregate(column: str, func: str) -> Callable:
        """
        Create an aggregation UDF.
        
        Args:
            column: The column to aggregate
            func: The aggregation function ('sum', 'avg', 'min', 'max', 'count')
            
        Returns:
            A function that performs the aggregation
        """
        def aggregator(rows: List[Dict[str, Any]]) -> Any:
            values = [row[column] for row in rows if column in row]
            if not values:
                return None
                
            if func == 'sum':
                return sum(values)
            elif func == 'avg':
                return sum(values) / len(values)
            elif func == 'min':
                return min(values)
            elif func == 'max':
                return max(values)
            elif func == 'count':
                return len(values)
            else:
                raise ValueError(f"Unknown aggregation function: {func}")
                
        return aggregator
    
    @staticmethod
    def filter_by(condition: Callable[[Dict[str, Any]], bool]) -> Callable:
        """
        Create a filtering UDF.
        
        Args:
            condition: A function that takes a row and returns True/False
            
        Returns:
            A function that filters rows based on the condition
        """
        def filter_func(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            return [row for row in rows if condition(row)]
        return filter_func
    
    @staticmethod
    def group_by(column: str, agg_func: Callable) -> Callable:
        """
        Create a grouping UDF with aggregation.
        
        Args:
            column: The column to group by
            agg_func: The aggregation function to apply to groups
            
        Returns:
            A function that groups rows and applies the aggregation
        """
        def grouper(rows: List[Dict[str, Any]]) -> Dict[Any, Any]:
            groups: Dict[Any, List[Dict[str, Any]]] = {}
            for row in rows:
                key = row.get(column)
                if key not in groups:
                    groups[key] = []
                groups[key].append(row)
            
            return {key: agg_func(group) for key, group in groups.items()}
        return grouper
    
    @staticmethod
    def window(column: str, window_size: int, func: Callable) -> Callable:
        """
        Create a window function UDF.
        
        Args:
            column: The column to apply the window function to
            window_size: The size of the window
            func: The function to apply to each window
            
        Returns:
            A function that applies the window operation
        """
        def window_func(rows: List[Dict[str, Any]]) -> List[Any]:
            values = [row[column] for row in rows if column in row]
            results = []
            
            for i in range(len(values) - window_size + 1):
                window = values[i:i + window_size]
                results.append(func(window))
            
            return results
        return window_func

# Register some common UDF outlines
def register_common_outlines():
    """Register commonly used UDF outlines with descriptive names."""
    
    # Average grade by age group
    @udf_registry.register(name="avg_grade_by_age")
    def avg_grade_by_age(rows):
        grouper = UDFOutline.group_by("age", 
            UDFOutline.aggregate("grade", "avg"))
        return grouper(rows)
    
    # Moving average of grades (window of 3)
    @udf_registry.register(name="grade_moving_avg")
    def grade_moving_avg(rows):
        window_avg = UDFOutline.window("grade", 3, 
            lambda x: sum(x) / len(x))
        return window_avg(rows)
    
    # High performers filter (grade > 85)
    @udf_registry.register(name="high_performers")
    def high_performers(rows):
        high_grade_filter = UDFOutline.filter_by(
            lambda row: row.get("grade", 0) > 85)
        return high_grade_filter(rows)
    
    # Count students by status
    @udf_registry.register(name="status_count")
    def status_count(rows):
        status_grouper = UDFOutline.group_by("status",
            UDFOutline.aggregate("name", "count"))
        return status_grouper(rows)

# Initialize common outlines
register_common_outlines() 