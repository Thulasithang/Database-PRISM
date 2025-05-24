from lark import Lark, Transformer, v_args

sql_grammar = r"""
start: select_stmt | insert_stmt | create_stmt

select_stmt: "SELECT" column_list "FROM" NAME ["WHERE" condition_list]
column_list: NAME ("," NAME)*
condition_list: condition (LOGIC_OP condition)*
condition: NAME COMP_OP value
value: STRING | SIGNED_NUMBER | NAME

insert_stmt: "INSERT" "INTO" NAME "(" column_list ")" "VALUES" "(" value_list ")"
value_list: value ("," value)*

create_stmt: "CREATE" "TABLE" NAME "(" create_column_list ")"
create_column_list: create_column ("," create_column)*
create_column: NAME TYPE

NAME: /[a-zA-Z_][a-zA-Z0-9_]*/
TYPE: "INT" | "TEXT"
COMP_OP: "=" | ">" | "<" | ">=" | "<=" | "!="
LOGIC_OP: "AND" | "OR"
STRING: /'[^']*'/ | /"[^"]*"/

%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%import common.WS
%ignore WS
"""


@v_args(inline=True)
class SQLTransformer(Transformer):
    def start(self, stmt): return stmt

    def select_stmt(self, cols, table, conds=None):
        return {
            "type": "select",
            "columns": cols,
            "from": str(table),
            "where": conds or []
        }

    def column_list(self, *cols): return [str(c) for c in cols]
    
    def condition(self, col, op, val):
        return {"left": str(col), "op": str(op), "right": val}

    def condition_list(self, *conds): return list(conds)

    def insert_stmt(self, table, columns, values):
        return {
            "type": "insert",
            "table": str(table),
            "columns": columns,
            "values": values
        }

    def value_list(self, *vals): return list(vals)

    def create_stmt(self, table, columns):
        print(f"Creating table: {table} with columns: {columns}")
        return {
            "type": "create_table",
            "table": str(table),
            "columns": columns
        }

    def create_column_list(self, *cols): return list(cols)

    def create_column(self, name, type_):
        return {"name": str(name), "datatype": str(type_)}

    def value(self, v):
        if isinstance(v, str):
            if v.startswith("'") or v.startswith('"'):
                return v[1:-1]  # strip quotes
            elif v.isdigit():
                return int(v)
            try:
                return float(v)
            except ValueError:
                return v
        elif isinstance(v):
            if v.type == "SIGNED_NUMBER":
                if '.' in v:
                    return float(v)
                else:
                    return int(v)
            elif v.type == "ESCAPED_STRING":
                return v.value[1:-1]
            else:
                return v.value
        return v



parser = Lark(sql_grammar, parser='lalr', transformer=SQLTransformer())

# sql1 = "SELECT name, age FROM users WHERE age >= 25 AND name != 'Alice'"
# sql2 = "INSERT INTO users (name, age) VALUES ('Bob', 30)"
# sql3 = "CREATE TABLE users (id INT, name TEXT)"

# print(parser.parse(sql1))
# print(parser.parse(sql2))
# print(parser.parse(sql3))
