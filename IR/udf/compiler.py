import llvmlite.binding as llvm
import llvmlite.ir as ir
from typing import Dict, Any, Callable, List, Tuple
import ctypes
import ast
import re

# Initialize LLVM
llvm.initialize()
llvm.initialize_native_target()
llvm.initialize_native_asmprinter()

class UDFCompiler:
    def __init__(self):
        self.module = ir.Module(name="udf_module")
        self.execution_engine = None
        self.target_machine = None
        self._init_targets()
    
    def __del__(self):
        """Clean up LLVM resources."""
        if self.execution_engine is not None:
            self.execution_engine.close()
    
    def _init_targets(self):
        """Initialize the compilation target."""
        target = llvm.Target.from_default_triple()
        self.target_machine = target.create_target_machine()
        self.module.triple = self.target_machine.triple
        self.module.data_layout = self.target_machine.target_data
        
    def compile_function(self, name: str, arg_types: list, return_type: str, body: str) -> Callable:
        """
        Compile a user-defined function to machine code.
        
        Args:
            name: Name of the function
            arg_types: List of argument types (e.g., ['int', 'float'])
            return_type: Return type of the function
            body: Function body in a simplified syntax
            
        Returns:
            A callable function object
        """
        try:
            # Create a new module for each function to avoid conflicts
            self.module = ir.Module(name=f"udf_module_{name}")
            self._init_targets()
            
            # Create function type
            if return_type == 'int':
                ret_type = ir.IntType(32)
            elif return_type == 'float':
                ret_type = ir.FloatType()
            else:
                raise ValueError(f"Unsupported return type: {return_type}")
                
            arg_ir_types = []
            for arg_type in arg_types:
                if arg_type == 'int':
                    arg_ir_types.append(ir.IntType(32))
                elif arg_type == 'float':
                    arg_ir_types.append(ir.FloatType())
                else:
                    raise ValueError(f"Unsupported argument type: {arg_type}")
            
            # Create function
            func_type = ir.FunctionType(ret_type, arg_ir_types)
            func = ir.Function(self.module, func_type, name=name)
            
            # Create a basic block
            block = func.append_basic_block(name="entry")
            builder = ir.IRBuilder(block)
            
            # Extract argument names from the body
            arg_names = self._extract_arg_names(body)
            
            # Create variable mapping
            variables = {}
            for arg_name, ir_arg in zip(arg_names, func.args):
                variables[arg_name] = ir_arg
                ir_arg.name = arg_name
            
            # Parse and compile the body
            result = self._compile_body(builder, body, variables, ret_type)
            builder.ret(result)
            
            # Optimize and compile to machine code
            return self._compile_ir_to_callable(func)
        except Exception as e:
            raise ValueError(f"Failed to compile function: {str(e)}")
    
    def _extract_arg_names(self, body: str) -> List[str]:
        """Extract argument names from the function body."""
        # Look for variable names that appear in expressions
        names = set()
        try:
            tree = ast.parse(self._clean_body(body))
            for node in ast.walk(tree):
                if isinstance(node, ast.Name):
                    if node.id not in {'True', 'False', 'None'}:
                        names.add(node.id)
        except:
            pass
        return sorted(list(names))
    
    def _clean_body(self, body: str) -> str:
        """Clean up the function body for parsing."""
        # Remove semicolons
        body = re.sub(r';', '', body)
        
        # Remove indentation
        lines = [line.strip() for line in body.split('\n')]
        lines = [line for line in lines if line]
        
        # Handle RETURN keyword
        cleaned_lines = []
        for line in lines:
            if line.upper().startswith('RETURN '):
                line = 'return ' + line[7:]
            cleaned_lines.append(line)
            
        return '\n'.join(cleaned_lines)
    
    def _compile_body(self, builder: ir.IRBuilder, body: str, variables: Dict[str, Any], return_type: ir.Type) -> ir.Value:
        """Compile the function body."""
        try:
            # Clean up the body
            cleaned_body = self._clean_body(body)
            
            # Parse the body
            body_ast = ast.parse(cleaned_body)
            
            # Handle multiple statements
            if isinstance(body_ast, ast.Module):
                statements = body_ast.body
                result = None
                for stmt in statements:
                    if isinstance(stmt, ast.Assign):
                        target = stmt.targets[0].id
                        value = self._compile_expression(builder, stmt.value, variables)
                        # Store intermediate results in alloca
                        ptr = builder.alloca(value.type, name=target)
                        builder.store(value, ptr)
                        variables[target] = builder.load(ptr)
                    elif isinstance(stmt, ast.Expr):
                        result = self._compile_expression(builder, stmt.value, variables)
                    elif isinstance(stmt, ast.Return):
                        result = self._compile_expression(builder, stmt.value, variables)
                        # Convert the result to the expected return type if needed
                        result = self._convert_type(builder, result, return_type)
                        break
                return result if result is not None else ir.Constant(ir.IntType(32), 0)
            
            raise ValueError("Invalid body structure")
        except Exception as e:
            raise ValueError(f"Failed to compile body: {e}")
    
    def _convert_type(self, builder: ir.IRBuilder, value: ir.Value, target_type: ir.Type) -> ir.Value:
        """Convert a value to the target type if needed."""
        if value.type == target_type:
            return value
            
        if isinstance(target_type, ir.IntType):
            if isinstance(value.type, ir.FloatType):
                return builder.fptosi(value, target_type)
            elif isinstance(value.type, ir.IntType):
                if value.type.width < target_type.width:
                    return builder.sext(value, target_type)
                else:
                    return builder.trunc(value, target_type)
        elif isinstance(target_type, ir.FloatType):
            if isinstance(value.type, ir.IntType):
                return builder.sitofp(value, target_type)
            
        raise ValueError(f"Cannot convert from {value.type} to {target_type}")
    
    def _compile_expression(self, builder: ir.IRBuilder, node: ast.AST, variables: Dict[str, Any]) -> ir.Value:
        """Compile a Python AST expression node to LLVM IR."""
        if isinstance(node, (ast.Num, ast.Constant)):
            # Handle numeric literals
            value = node.n if isinstance(node, ast.Num) else node.value
            if isinstance(value, int):
                return ir.Constant(ir.IntType(32), value)
            else:
                # Convert Python float to LLVM float constant
                return ir.Constant(ir.FloatType(), float(value))
        elif isinstance(node, ast.Name):
            # Handle variable references
            if node.id not in variables:
                raise ValueError(f"Undefined variable: {node.id}")
            return variables[node.id]
        elif isinstance(node, ast.BinOp):
            # Handle binary operations
            left = self._compile_expression(builder, node.left, variables)
            right = self._compile_expression(builder, node.right, variables)
            
            # Convert types if needed
            if isinstance(left.type, ir.FloatType) and isinstance(right.type, ir.IntType):
                right = builder.sitofp(right, ir.FloatType())
            elif isinstance(right.type, ir.FloatType) and isinstance(left.type, ir.IntType):
                left = builder.sitofp(left, ir.FloatType())
            
            # Ensure both operands have the same type
            if left.type != right.type:
                raise ValueError(f"Type mismatch in binary operation: {left.type} vs {right.type}")
            
            if isinstance(node.op, ast.Add):
                return builder.fadd(left, right) if isinstance(left.type, ir.FloatType) else builder.add(left, right)
            elif isinstance(node.op, ast.Sub):
                return builder.fsub(left, right) if isinstance(left.type, ir.FloatType) else builder.sub(left, right)
            elif isinstance(node.op, ast.Mult):
                return builder.fmul(left, right) if isinstance(left.type, ir.FloatType) else builder.mul(left, right)
            elif isinstance(node.op, ast.Div):
                if isinstance(left.type, ir.IntType):
                    return builder.sdiv(left, right)
                else:
                    return builder.fdiv(left, right)
            else:
                raise ValueError(f"Unsupported binary operator: {type(node.op).__name__}")
        elif isinstance(node, ast.Compare):
            # Handle comparison operations
            left = self._compile_expression(builder, node.left, variables)
            right = self._compile_expression(builder, node.comparators[0], variables)
            
            # Convert types if needed
            if isinstance(left.type, ir.FloatType) and isinstance(right.type, ir.IntType):
                right = builder.sitofp(right, ir.FloatType())
            elif isinstance(right.type, ir.FloatType) and isinstance(left.type, ir.IntType):
                left = builder.sitofp(left, ir.FloatType())
            
            # Ensure both operands have the same type
            if left.type != right.type:
                raise ValueError(f"Type mismatch in comparison: {left.type} vs {right.type}")
            
            op = node.ops[0]
            result = None
            
            if isinstance(left.type, ir.IntType):
                if isinstance(op, ast.GtE):
                    result = builder.icmp_signed('>=', left, right)
                elif isinstance(op, ast.LtE):
                    result = builder.icmp_signed('<=', left, right)
                elif isinstance(op, ast.Gt):
                    result = builder.icmp_signed('>', left, right)
                elif isinstance(op, ast.Lt):
                    result = builder.icmp_signed('<', left, right)
                elif isinstance(op, ast.Eq):
                    result = builder.icmp_signed('==', left, right)
                elif isinstance(op, ast.NotEq):
                    result = builder.icmp_signed('!=', left, right)
            else:
                if isinstance(op, ast.GtE):
                    result = builder.fcmp_ordered('>=', left, right)
                elif isinstance(op, ast.LtE):
                    result = builder.fcmp_ordered('<=', left, right)
                elif isinstance(op, ast.Gt):
                    result = builder.fcmp_ordered('>', left, right)
                elif isinstance(op, ast.Lt):
                    result = builder.fcmp_ordered('<', left, right)
                elif isinstance(op, ast.Eq):
                    result = builder.fcmp_ordered('==', left, right)
                elif isinstance(op, ast.NotEq):
                    result = builder.fcmp_ordered('!=', left, right)
            
            if result is None:
                raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")
                
            # Convert boolean result to integer
            return builder.zext(result, ir.IntType(32))
        else:
            raise ValueError(f"Unsupported expression type: {type(node).__name__}")
    
    def _compile_ir_to_callable(self, func: ir.Function) -> Callable:
        """
        Compile the LLVM IR to machine code and return a callable.
        """
        try:
            # Create a module pass manager
            pm = llvm.create_module_pass_manager()
            
            # Convert IR to machine code
            llvm_ir = str(self.module)
            mod = llvm.parse_assembly(llvm_ir)
            mod.verify()
            
            # Create execution engine
            if self.execution_engine is not None:
                self.execution_engine.close()
            
            self.execution_engine = llvm.create_mcjit_compiler(mod, self.target_machine)
            self.execution_engine.finalize_object()
            
            # Get function pointer
            func_ptr = self.execution_engine.get_function_address(func.name)
            
            # Create a ctypes function with the appropriate signature
            if isinstance(func.function_type.return_type, ir.IntType):
                return_type = ctypes.c_int32
            else:
                return_type = ctypes.c_float
                
            arg_types = []
            for arg in func.args:
                if isinstance(arg.type, ir.IntType):
                    arg_types.append(ctypes.c_int32)
                else:
                    arg_types.append(ctypes.c_float)
                    
            callable_func = ctypes.CFUNCTYPE(return_type, *arg_types)(func_ptr)
            return callable_func
        except Exception as e:
            if self.execution_engine is not None:
                self.execution_engine.close()
                self.execution_engine = None
            raise ValueError(f"Failed to compile function: {str(e)}") 