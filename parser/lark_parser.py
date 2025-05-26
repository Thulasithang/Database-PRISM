from lark import Lark, Transformer, v_args

sql_grammar = r"""
start: stmt (";" stmt)* ";"?

stmt: select_stmt | insert_stmt | create_table_stmt | create_function_stmt

select_stmt: "SELECT" column_list "FROM" NAME where_clause?
where_clause: "WHERE" condition
condition: function_call | comparison
comparison: expr OPERATOR expr
OPERATOR: ">=" | "<=" | ">" | "<" | "=" | "!="

column_list: column ("," column)*
column: NAME | function_call

insert_stmt: "INSERT" "INTO" NAME "(" column_list ")" "VALUES" "(" value_list ")"
value_list: value ("," value)*

create_table_stmt: "CREATE" "TABLE" NAME "(" create_column_list ")"
create_column_list: create_column ("," create_column)*
create_column: NAME TYPE

create_function_stmt: "CREATE" "FUNCTION" NAME "(" param_list ")" "RETURNS" TYPE function_body
param_list: param ("," param)*
param: NAME TYPE
function_body: "BEGIN" stmt_list "END" ";"
stmt_list: if_stmt | return_stmt
if_stmt: "IF" comparison "THEN" return_stmt "ELSE" return_stmt "END" "IF" ";"
return_stmt: "RETURN" expr ";"

function_call: NAME "(" value_list? ")"
expr: arithmetic_expr | value
arithmetic_expr: value ARITH_OP value
ARITH_OP: "+" | "-" | "*" | "/"
value: STRING | SIGNED_NUMBER | NAME | function_call | BOOL
BOOL: "true" | "false"

NAME: /[a-zA-Z_][a-zA-Z0-9_]*/
TYPE: "INT" | "TEXT" | "FLOAT" | "BOOL"
STRING: /'[^']*'/ | /"[^"]*"/

%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%import common.WS
%ignore WS
"""

@v_args(inline=True)
class SQLTransformer(Transformer):
    def start(self, *args):
        return [stmt for stmt in args if stmt is not None]

    def stmt(self, stmt):
        return stmt

    def select_stmt(self, *args):
        cols = args[0]
        table = args[1]
        where = args[2] if len(args) > 2 else None
        return {
            "type": "select",
            "columns": cols,
            "from": str(table),
            "where": where
        }

    def where_clause(self, condition):
        return condition

    def condition(self, cond):
        return cond

    def comparison(self, left, op, right):
        return {
            "type": "comparison",
            "left": left,
            "op": str(op),
            "right": right
        }

    def OPERATOR(self, op):
        return str(op)

    def column_list(self, *items): 
        return list(items)

    def column(self, item):
        return item

    def insert_stmt(self, *args):
        table = None
        columns = None
        values = None
        
        for arg in args:
            if isinstance(arg, str) and table is None:
                table = arg
            elif isinstance(arg, list):
                if columns is None:
                    columns = arg
                elif values is None:
                    values = arg
                    
        return {
            "type": "insert",
            "table": str(table),
            "columns": columns,
            "values": values
        }

    def value_list(self, *vals): 
        return list(vals)

    def create_table_stmt(self, *args):
        table = None
        columns = None
        for i, arg in enumerate(args):
            if isinstance(arg, str) and table is None:
                table = arg
            elif isinstance(arg, list) and columns is None:
                columns = arg
                
        return {
            "type": "create_table",
            "table": str(table),
            "columns": columns
        }

    def create_column_list(self, *cols): 
        return list(cols)

    def create_column(self, name, type_):
        return {"name": str(name), "datatype": str(type_)}

    def create_function_stmt(self, name, params, return_type, body):
        return {
            "type": "create_function",
            "name": str(name),
            "params": params,
            "return_type": str(return_type),
            "body": body
        }

    def param_list(self, *params):
        return list(params)

    def param(self, name, type_):
        return {"name": str(name), "type": str(type_)}

    def function_body(self, stmt_list):
        return stmt_list

    def stmt_list(self, stmt):
        return stmt

    def if_stmt(self, condition, then_stmt, else_stmt):
        return {
            "type": "if_stmt",
            "condition": condition,
            "then": then_stmt,
            "else": else_stmt
        }

    def return_stmt(self, value):
        return {
            "type": "return_stmt",
            "value": value
        }

    def expr(self, value):
        return value

    def arithmetic_expr(self, left, op, right):
        return {
            "type": "arithmetic",
            "left": left,
            "op": str(op),
            "right": right
        }

    def ARITH_OP(self, op):
        return str(op)

    def function_call(self, name, args=None):
        return {
            "type": "function_call",
            "function_name": str(name),
            "arguments": args if args is not None else []
        }

    def BOOL(self, value):
        return str(value).lower()

    def value(self, v):
        if isinstance(v, str):
            if v.startswith("'") or v.startswith('"'):
                return v[1:-1]  # strip quotes
            elif v.lower() in ('true', 'false'):
                return v.lower()  # preserve boolean literals
            try:
                if '.' in v:
                    return float(v)
                return int(v)
            except ValueError:
                return v
        elif hasattr(v, 'value'):  # Handle Token objects
            try:
                if v.value.lower() in ('true', 'false'):
                    return v.value.lower()
                elif '.' in v.value:
                    return float(v.value)
                return int(v.value)
            except ValueError:
                return v.value
        elif isinstance(v, dict):  # Handle function calls
            return v
        return v

parser = Lark(sql_grammar, parser='lalr', transformer=SQLTransformer())

# sql1 = "SELECT name, age FROM users WHERE age >= 25 AND name != 'Alice'"
# sql2 = "INSERT INTO users (name, age) VALUES ('Bob', 30)"
# sql3 = "CREATE TABLE users (id INT, name TEXT)"

# print(parser.parse(sql1))
# print(parser.parse(sql2))
# print(parser.parse(sql3))
