from typing import Dict, List, Tuple, Union
import re

class UDFParser:
    def __init__(self):
        self.keywords = {'int', 'float', 'bool', 'return', 'if', 'else', 'while'}
        self.valid_types = {'int', 'float', 'bool'}
        
    def parse_function_definition(self, udf_text: str) -> Dict[str, Union[str, List[Dict[str, str]], str]]:
        """
        Parse a UDF definition into a structured format.
        
        Example input:
        CREATE FUNCTION add_numbers(a int, b int) RETURNS int
        BEGIN
            RETURN a + b;
        END;
        
        Or with if-else:
        CREATE FUNCTION is_adult(age int) RETURNS bool
        BEGIN
            IF age >= 18 THEN
                RETURN true;
            ELSE
                RETURN false;
            END IF;
        END;
        
        Returns:
            Dict containing function name, params, return_type, and body
        """
        # Clean up input text
        udf_text = udf_text.strip()
        
        # Extract function signature with more flexible whitespace handling
        signature_match = re.match(
            r'\s*CREATE\s+FUNCTION\s+(\w+)\s*\((.*?)\)\s*RETURNS\s+(\w+)',
            udf_text,
            re.IGNORECASE | re.DOTALL
        )
        
        if not signature_match:
            raise ValueError("Invalid function definition syntax")
            
        name = signature_match.group(1)
        args_str = signature_match.group(2)
        return_type = signature_match.group(3).lower()
        
        # Validate return type
        if return_type not in self.valid_types:
            raise ValueError(f"Invalid return type: {return_type}. Must be one of: {', '.join(self.valid_types)}")
        
        # Parse arguments
        params = self._parse_arguments(args_str)
        
        # Extract function body with more flexible whitespace handling
        body_match = re.search(
            r'BEGIN\s*(.*?)\s*END\s*;',
            udf_text,
            re.IGNORECASE | re.DOTALL
        )
        
        if not body_match:
            raise ValueError("Function body not found")
            
        body = body_match.group(1).strip()
        
        # Handle if-else conditions
        if_match = re.match(
            r'\s*IF\s+(.*?)\s+THEN\s*(.*?)\s*ELSE\s*(.*?)\s*END\s*IF\s*;',
            body,
            re.IGNORECASE | re.DOTALL
        )
        
        if if_match:
            condition = if_match.group(1).strip()
            then_body = if_match.group(2).strip()
            else_body = if_match.group(3).strip()
            
            # Extract return values from then and else bodies
            then_return = re.match(r'RETURN\s+(.*?)\s*;', then_body, re.IGNORECASE)
            else_return = re.match(r'RETURN\s+(.*?)\s*;', else_body, re.IGNORECASE)
            
            if not then_return or not else_return:
                raise ValueError("Both IF and ELSE blocks must contain RETURN statements")
                
            # Split condition into parts
            condition_parts = condition.split()
            if len(condition_parts) != 3:
                raise ValueError("Invalid condition format. Expected: <variable> <operator> <value>")
                
            # Create structured body
            body = {
                "type": "if_stmt",
                "condition": {
                    "type": "comparison",
                    "left": condition_parts[0],
                    "op": condition_parts[1],
                    "right": condition_parts[2]
                },
                "then": {
                    "type": "return_stmt",
                    "value": then_return.group(1).strip()
                },
                "else": {
                    "type": "return_stmt",
                    "value": else_return.group(1).strip()
                }
            }
        else:
            # Handle simple return statement
            return_match = re.match(r'RETURN\s+(.*?)\s*;', body, re.IGNORECASE)
            if not return_match:
                raise ValueError("Invalid function body: must contain a RETURN statement")
            
            body = {
                "type": "return_stmt",
                "value": return_match.group(1).strip()
            }
        
        return {
            'name': name,
            'params': params,
            'return_type': return_type,
            'body': body
        }
        
    def _parse_arguments(self, args_str: str) -> List[Dict[str, str]]:
        """Parse function arguments into a list of dictionaries."""
        if not args_str.strip():
            return []
            
        args = []
        for arg in args_str.split(','):
            arg = arg.strip()
            if not arg:
                continue
                
            # Handle more flexible whitespace in argument definitions
            parts = re.match(r'\s*(\w+)\s+(\w+)\s*', arg)
            if not parts:
                raise ValueError(f"Invalid argument format: {arg}")
                
            name = parts.group(1)
            type_name = parts.group(2).lower()
            
            if type_name not in self.valid_types:
                raise ValueError(f"Invalid argument type: {type_name}. Must be one of: {', '.join(self.valid_types)}")
                
            args.append({
                'name': name,
                'type': type_name
            })
            
        return args
        
    def parse_body(self, body: str) -> List[Dict]:
        """
        Parse the function body into a list of operations.
        This is a simplified parser that handles basic arithmetic and return statements.
        """
        operations = []
        
        # Split into statements and clean up whitespace
        statements = [s.strip() for s in body.split(';') if s.strip()]
        
        for stmt in statements:
            if stmt.upper().startswith('RETURN'):
                expr = stmt[6:].strip()  # Remove 'RETURN' keyword
                operations.append({
                    'type': 'return',
                    'expression': self._parse_expression(expr)
                })
            elif '=' in stmt:
                var, expr = [s.strip() for s in stmt.split('=', 1)]
                operations.append({
                    'type': 'assignment',
                    'target': var,
                    'expression': self._parse_expression(expr)
                })
                
        return operations
        
    def _parse_expression(self, expr: str) -> Dict:
        """
        Parse a simple arithmetic expression.
        This is a basic implementation that handles only simple operations.
        """
        # Clean up input
        expr = expr.strip()
        
        # Check for basic arithmetic operations
        for op in ['+', '-', '*', '/', '>=', '<=', '>', '<', '==', '!=']:
            if op in expr:
                left, right = expr.split(op, 1)
                return {
                    'type': 'binary_op',
                    'operator': op,
                    'left': self._parse_expression(left.strip()),
                    'right': self._parse_expression(right.strip())
                }
                
        # If no operators found, it's either a number or a variable
        try:
            value = float(expr)
            return {
                'type': 'literal',
                'value': value,
                'value_type': 'float' if '.' in expr else 'int'
            }
        except ValueError:
            return {
                'type': 'variable',
                'name': expr.strip()
            } 