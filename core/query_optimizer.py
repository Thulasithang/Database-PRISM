from typing import Any, Callable, Dict, List, Optional, Set
from core.udf import udf_registry
from core.db_storage import db_storage
import re
from collections import defaultdict

class QueryOptimizer:
    """
    A query optimizer that identifies repeated expressions and converts them into UDFs.
    """
    
    def __init__(self):
        self.expression_cache: Dict[str, int] = {}  # Track expression frequency
        self.generated_udfs: Dict[str, str] = {}  # Map normalized expressions to UDF names
        
        # Load existing UDFs from database
        for udf in db_storage.list_udfs():
            self._register_udf_from_db(udf)
    
    def _register_udf_from_db(self, udf_info: Dict[str, Any]) -> None:
        """Register a UDF from database information."""
        def udf_function(*args: Any) -> Any:
            # Map parameters to their values
            values = dict(zip(udf_info["parameters"], args))
            
            # Replace parameters with values in the expression
            result_expr = udf_info["expression"]
            for param, value in values.items():
                result_expr = result_expr.replace(param, str(value))
            
            # Evaluate the expression
            return eval(result_expr)
        
        # Register the UDF
        udf_registry.register(name=udf_info["name"])(udf_function)
        # Store the normalized expression to UDF name mapping
        normalized = self._normalize_expression(udf_info["expression"])
        self.generated_udfs[normalized] = udf_info["name"]
    
    def analyze_expression(self, expression: str) -> None:
        """Record an expression for frequency analysis."""
        # Use the same normalization as other methods
        normalized = self._normalize_expression(expression)
        
        # Track the expression frequency
        self.expression_cache[normalized] = self.expression_cache.get(normalized, 0) + 1
        
        # Debug output
        print(f"Analyzing expression: {expression}")
        print(f"Normalized form: {normalized}")
        print(f"Current frequency: {self.expression_cache[normalized]}")
    
    def _normalize_expression(self, expr: str) -> str:
        """Normalize an expression by removing extra spaces and standardizing format."""
        # Remove all spaces and convert to lowercase for case-insensitive comparison
        normalized = ''.join(expr.lower().split())
        
        # Add spaces around operators and parentheses
        normalized = re.sub(r'([+\-*/()=<>])', r' \1 ', normalized)
        
        # Handle special cases for operators
        normalized = re.sub(r'\s*([<>])\s*=\s*', r' \1= ', normalized)  # <=, >=
        normalized = re.sub(r'\s*<\s*>\s*', r' <> ', normalized)  # <>
        
        # Normalize spaces
        normalized = ' '.join(normalized.split())
        
        return normalized
    
    def should_create_udf(self, expression: str, min_occurrences: int = 2) -> bool:
        """Determine if an expression should be converted to a UDF."""
        normalized = self._normalize_expression(expression)
        frequency = self.expression_cache.get(normalized, 0)
        
        # Debug output
        print(f"Checking expression: {expression}")
        print(f"Normalized form: {normalized}")
        print(f"Current frequency: {frequency}")
        print(f"Already has UDF: {normalized in self.generated_udfs}")
        
        return frequency >= min_occurrences and normalized not in self.generated_udfs
    
    def _extract_parameters(self, expr: str) -> List[str]:
        """Extract parameters from an expression, handling table aliases."""
        # Find all potential parameters (words that might be columns)
        params = re.findall(r'\b(?!AND|OR|NOT|CASE|WHEN|THEN|ELSE|END\b)\w+(?:\.\w+)?\b', expr)
        
        # Filter out numeric values and handle table aliases
        clean_params = []
        for param in params:
            if not param.replace('.', '').isdigit():  # Not a number
                if '.' in param:  # Has table alias
                    alias, col = param.split('.')
                    if col not in clean_params:  # Only add unique column names
                        clean_params.append(col)
                else:
                    if param not in clean_params:  # Only add unique names
                        clean_params.append(param)
        
        return clean_params  # Keep original order for parameter consistency

    def create_udf_from_expression(self, expression: str) -> str:
        """
        Create a UDF from an expression and save it to the database.
        
        Args:
            expression: The expression to convert
            
        Returns:
            Name of the created UDF
        """
        # Extract parameters first
        params = self._extract_parameters(expression)
        
        # Normalize the expression and remove table aliases
        normalized = self._normalize_expression(expression)
        for param in re.findall(r'\b\w+\.\w+\b', expression):
            alias, col = param.split('.')
            normalized = normalized.replace(param, col)
        
        # Check if we already have a UDF for this expression
        if normalized in self.generated_udfs:
            return self.generated_udfs[normalized]
        
        # Generate a meaningful name for the UDF based on the operation type
        if '*' in normalized and '+' in normalized:
            prefix = 'calc_total'
        elif '*' in normalized:
            prefix = 'calc_mult'
        elif '+' in normalized or '-' in normalized:
            prefix = 'calc_sum'
        else:
            prefix = 'calc'
        
        udf_name = f"{prefix}_{hash(normalized) & 0xFFFFFF:x}"
        
        # Create the UDF function
        def create_udf_function(expr: str, param_list: List[str]) -> Callable:
            def udf_function(*args: Any) -> Any:
                # Map parameters to their values
                values = dict(zip(param_list, args))
                
                # Replace parameters with values in the expression
                result_expr = expr
                for param in param_list:
                    # Replace both aliased and non-aliased versions
                    result_expr = re.sub(rf'\b\w+\.{param}\b', str(values[param]), result_expr)
                    result_expr = re.sub(rf'\b{param}\b(?!\w)', str(values[param]), result_expr)
                
                # Evaluate the expression
                return eval(result_expr)
            
            return udf_function
        
        # Register the UDF
        udf = create_udf_function(expression, params)
        udf_registry.register(name=udf_name)(udf)
        
        # Save to database
        db_storage.save_udf(udf_name, normalized, params)
        
        # Store the normalized expression to UDF name mapping
        self.generated_udfs[normalized] = udf_name
        return udf_name
    
    def optimize_query(self, query: str) -> str:
        """
        Optimize a query by identifying and extracting common expressions into UDFs.
        """
        # Track expressions and their frequencies
        expr_frequencies = defaultdict(int)
        expr_locations = []  # List of (start, end, expr, alias) tuples
        normalized_exprs = {}  # Map of normalized expressions to original expressions
        
        # Find all expressions in the query
        for match in re.finditer(r'\([^()]+\)(?:\s+as\s+(\w+))?', query, re.IGNORECASE):
            expr = match.group(0)
            alias = match.group(1)  # This will be None if no alias
            clean_expr = expr[1:-1].strip()  # Remove outer parentheses
            
            if self._is_valid_expression(clean_expr):
                normalized = self._normalize_expression(clean_expr)
                expr_frequencies[normalized] += 1
                expr_locations.append((match.start(), match.end(), clean_expr, alias))
                normalized_exprs[normalized] = clean_expr
                
                print(f"Analyzing expression: {clean_expr}")
                print(f"Normalized form: {normalized}")
                print(f"Current frequency: {expr_frequencies[normalized]}")
        
        # Create UDFs for frequently occurring expressions
        replacements = []  # List of (start, end, replacement) tuples
        for normalized, freq in expr_frequencies.items():
            if self._should_create_udf(normalized, freq):
                # Create the UDF
                expr = normalized_exprs[normalized]
                udf_name = self.create_udf_from_expression(expr)
                
                # Find all occurrences of this expression
                for start, end, expr, alias in expr_locations:
                    if self._normalize_expression(expr) == normalized:
                        # Extract parameters with table aliases
                        params = []
                        for param in re.findall(r'\b(?!AND|OR|NOT|CASE|WHEN|THEN|ELSE|END\b)\w+(?:\.\w+)?\b', expr):
                            if not param.replace('.', '').isdigit():
                                params.append(param)
                        
                        # Create UDF call with proper parameter order
                        udf_call = f"{udf_name}({', '.join(params)})"
                        
                        # Add alias if it exists
                        if alias:
                            udf_call = f"({udf_call}) as {alias}"
                        
                        replacements.append((start, end, udf_call))
        
        # Apply replacements in reverse order
        result = query
        for start, end, replacement in sorted(replacements, reverse=True):
            # Extract context
            prefix = result[:start].rstrip()
            suffix = result[end:].lstrip()
            
            # Determine if we need parentheses
            needs_parens = False
            if (suffix and suffix[0] in '+-*/=<>') or \
               (prefix and prefix[-1] in '+-*/=<>'):
                needs_parens = True
            
            # Apply replacement
            final_replacement = f"({replacement})" if needs_parens and 'as' not in replacement else replacement
            result = prefix + final_replacement + suffix
        
        return result

    def _should_create_udf(self, normalized_expr: str, frequency: int) -> bool:
        """
        Determine if a UDF should be created for an expression.
        
        Args:
            normalized_expr: The normalized form of the expression
            frequency: How many times the expression appears
            
        Returns:
            bool: True if a UDF should be created
        """
        # Don't create if already exists
        if normalized_expr in self.generated_udfs:
            return False
            
        # High frequency expressions should always be converted
        if frequency >= 4:
            return True
            
        # Medium frequency expressions (2-3 times) need some complexity
        if frequency >= 2:
            # Check if expression has operators and operands
            operators = re.findall(r'[+\-*/]', normalized_expr)
            operands = re.findall(r'\b[a-zA-Z]\w*\b', normalized_expr)
            
            # Create UDF if expression has:
            # - Multiple operators OR
            # - One operator but involves column references (not just literals)
            return len(operators) > 1 or (len(operators) == 1 and len(operands) >= 2)
            
        return False

    def _is_valid_expression(self, expr: str) -> bool:
        """
        Check if an expression is valid for UDF creation.
        
        Args:
            expr: The expression to validate
            
        Returns:
            bool: True if the expression is valid for UDF creation
        """
        # Must contain at least one operator
        if not any(op in expr for op in ['+', '-', '*', '/', '>', '<', '=', '!=', '>=', '<=']):
            return False
            
        # Must not contain SQL keywords
        sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'JOIN', 'ON',
            'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN'
        ]
        if any(keyword in expr.upper() for keyword in sql_keywords):
            return False
            
        # Must not be just a simple column reference
        if not re.search(r'[^a-zA-Z0-9_\.]', expr):
            return False
            
        return True

# Example usage
def optimize_example():
    # Create an instance of the optimizer
    optimizer = QueryOptimizer()
    
    # Example query with repeated expressions
    query = """
    SELECT 
        id,
        (price * quantity) + tax - discount AS total_cost
    FROM orders
    WHERE ((price * quantity) + tax - discount) > 1000;
    """
    
    # Optimize the query
    optimized = optimizer.optimize_query(query)
    print("Original query:")
    print(query)
    print("\nOptimized query:")
    print(optimized)
    
    # Show generated UDFs
    print("\nGenerated UDFs:")
    for udf_name in optimizer.generated_udfs:
        print(f"- {udf_name}")

if __name__ == "__main__":
    optimize_example() 