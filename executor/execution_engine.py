from collections import defaultdict
from table_loader import tables

def fetch_table_rows(table_name):
    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' not found.")
    schema = tables[table_name]["columns"]
    rows = tables[table_name]["rows"]
    return schema, rows

def safe_eval(expr, row_dict):
    allowed_builtins = {
        "min": min, "max": max, "abs": abs, "round": round,
        "int": int, "float": float, "bool": bool, "str": str
    }

    # Replace dots in keys to make them valid identifiers
    safe_row_dict = {k.replace('.', '_'): v for k, v in row_dict.items()}
    expr = expr.replace('.', '_')  # Update expression accordingly

    try:
        return eval(expr, {"__builtins__": allowed_builtins}, safe_row_dict)
    except Exception as e:
        raise RuntimeError(f"Failed to evaluate expression '{expr}': {e}")


def evaluate_condition(cond_expr, row_dict):
    if cond_expr is None:
        return True
    if isinstance(cond_expr, dict) and cond_expr.get("type") == "subquery":
        raise NotImplementedError("Subqueries not supported in this version.")
    return bool(safe_eval(cond_expr, row_dict))

def join_tables(left_table_name, left_schema, left_rows,
                right_table_name, right_schema, right_rows,
                join_keys, join_type="inner"):
    left_schema_prefixed = [f"{left_table_name}.{col}" for col in left_schema]
    right_schema_prefixed = [f"{right_table_name}.{col}" for col in right_schema]
    joined_schema = left_schema_prefixed + right_schema_prefixed

    right_index = defaultdict(list)
    right_key_positions = [right_schema.index(rcol) for _, rcol in join_keys]
    left_key_positions = [left_schema.index(lcol) for lcol, _ in join_keys]

    for rrow in right_rows:
        key = tuple(rrow[pos] for pos in right_key_positions)
        right_index[key].append(rrow)

    joined_rows = []
    for lrow in left_rows:
        key = tuple(lrow[pos] for pos in left_key_positions)
        matched_rows = right_index.get(key, [])
        if matched_rows:
            for rrow in matched_rows:
                row_dict = {f"{left_table_name}.{col}": val for col, val in zip(left_schema, lrow)}
                row_dict.update({f"{right_table_name}.{col}": val for col, val in zip(right_schema, rrow)})
                joined_rows.append(row_dict)
        elif join_type == "left":
            row_dict = {f"{left_table_name}.{col}": val for col, val in zip(left_schema, lrow)}
            row_dict.update({f"{right_table_name}.{col}": None for col in right_schema})
            joined_rows.append(row_dict)

    return joined_schema, joined_rows

def group_by(rows, schema, group_cols, agg_columns):
    col_idx = {c: i for i, c in enumerate(schema)}
    groups = defaultdict(list)
    for row in rows:
        key = tuple(row[col_idx[c]] for c in group_cols)
        groups[key].append(row)

    result_rows = []
    for key_vals, group_rows in groups.items():
        agg_results = {}
        for agg in agg_columns:
            expr = agg["expr"]
            alias = agg["alias"]
            agg_type = agg["agg"].upper()

            vals = [safe_eval(expr, {col: row[col_idx[col]] for col in schema}) for row in group_rows]

            if agg_type == "SUM":
                agg_val = sum(vals)
            elif agg_type == "COUNT":
                agg_val = len(vals)
            elif agg_type == "AVG":
                agg_val = sum(vals) / len(vals) if vals else 0
            elif agg_type == "MIN":
                agg_val = min(vals)
            elif agg_type == "MAX":
                agg_val = max(vals)
            else:
                raise NotImplementedError(f"Aggregate {agg_type} not supported")

            agg_results[alias] = agg_val

        result_rows.append(list(key_vals) + [agg_results[agg["alias"]] for agg in agg_columns])

    result_schema = group_cols + [agg["alias"] for agg in agg_columns]
    return result_schema, result_rows

def evaluate_columns(rows, schema, columns):
    results = []
    for row in rows:
        row_dict = dict(zip(schema, row))
        result_row = {}
        for col in columns:
            val = safe_eval(col["expr"], row_dict)
            alias = col.get("alias") or col["expr"]
            result_row[alias] = val
        results.append(result_row)
    return results

def order_by(rows, order_cols):
    for col, asc in reversed(order_cols):
        rows.sort(key=lambda r: r.get(col), reverse=not asc)
    return rows

def limit_offset(rows, limit=None, offset=None):
    if offset:
        rows = rows[offset:]
    if limit:
        rows = rows[:limit]
    return rows

def execute_select(ir):
    from_clause = ir["from"]
    if isinstance(from_clause, str):
        schema, rows = fetch_table_rows(from_clause)
        row_dicts = [dict(zip(schema, row)) for row in rows]
    else:
        left_table = from_clause["left"]
        right_table = from_clause["right"]
        join_keys = from_clause["on"]
        join_type = from_clause.get("join_type", "inner")

        left_schema, left_rows = fetch_table_rows(left_table)
        right_schema, right_rows = fetch_table_rows(right_table)

        schema, row_dicts = join_tables(left_table, left_schema, left_rows,
                                        right_table, right_schema, right_rows,
                                        join_keys, join_type)

    filtered = [row for row in row_dicts if evaluate_condition(ir.get("where"), row)]

    group_by_cols = ir.get("group_by")
    agg_columns = [c for c in ir["columns"] if "agg" in c]

    if group_by_cols:
        schema, rows = group_by(
            [[row[col] for col in schema] for row in filtered],
            schema, group_by_cols, agg_columns
        )
        results = [dict(zip(schema, row)) for row in rows]
    else:
        results = evaluate_columns(
            [[row[col] for col in schema] for row in filtered], schema, ir["columns"]
        )

    if "order_by" in ir:
        results = order_by(results, ir["order_by"])

    results = limit_offset(results, ir.get("limit"), ir.get("offset"))

    if results and isinstance(results[0], dict):
        schema = list(results[0].keys())
        rows = [list(row.values()) for row in results]
    else:
        schema = []
        rows = results

    return schema, rows

def execute_insert(ir):
    table_name = ir["into"]
    columns = ir.get("columns")
    values = ir["values"]

    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' not found.")

    schema = tables[table_name]["columns"]
    new_row = [None] * len(schema) if columns else values

    if columns:
        col_idx = {col: i for i, col in enumerate(schema)}
        for col, val in zip(columns, values):
            if col not in col_idx:
                raise ValueError(f"Column '{col}' not found.")
            new_row[col_idx[col]] = val

    tables[table_name]["rows"].append(new_row)
    return {"status": "OK", "inserted": new_row}

def execute_update(ir):
    table_name = ir["table"]
    set_exprs = ir["set"]
    where_expr = ir.get("where")

    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' not found.")

    schema = tables[table_name]["columns"]
    rows = tables[table_name]["rows"]
    col_idx = {col: i for i, col in enumerate(schema)}

    updated_count = 0
    for row in rows:
        row_dict = dict(zip(schema, row))
        if evaluate_condition(where_expr, row_dict):
            for col, expr in set_exprs.items():
                if col not in col_idx:
                    raise ValueError(f"Column '{col}' not found.")
                row[col_idx[col]] = safe_eval(expr, row_dict)
            updated_count += 1

    return {"status": "OK", "updated": updated_count}

def execute_delete(ir):
    table_name = ir["table"]
    where_expr = ir.get("where")

    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' not found.")

    schema = tables[table_name]["columns"]
    rows = tables[table_name]["rows"]

    new_rows = []
    deleted_count = 0
    for row in rows:
        row_dict = dict(zip(schema, row))
        if not evaluate_condition(where_expr, row_dict):
            new_rows.append(row)
        else:
            deleted_count += 1

    tables[table_name]["rows"] = new_rows
    return {"status": "OK", "deleted": deleted_count}

def execute_create_table(ir):
    table_name = ir["name"]
    columns = ir["columns"]
    if table_name in tables:
        raise ValueError(f"Table '{table_name}' already exists.")
    tables[table_name] = {"columns": columns, "rows": []}
    return {"status": "OK", "created": table_name}

def execute_drop_table(ir):
    table_name = ir["name"]
    if table_name in tables:
        del tables[table_name]
        return {"status": "OK", "dropped": table_name}
    raise ValueError(f"Table '{table_name}' not found.")

def execute_rename_table(ir):
    old_name = ir["old_name"]
    new_name = ir["new_name"]
    if old_name not in tables:
        raise ValueError(f"Table '{old_name}' not found.")
    if new_name in tables:
        raise ValueError(f"Table '{new_name}' already exists.")
    tables[new_name] = tables.pop(old_name)
    return {"status": "OK", "renamed": f"{old_name} -> {new_name}"}

# def execute_create_view(ir):
#     view_name = ir["name"]
#     select_ir = ir["select"]
#     views[view_name] = select_ir
#     return {"status": "OK", "view": view_name}

def execute_show_table(ir):
    tables_list = sorted(tables.keys())
    schema = ["table_name"]
    rows = [[t] for t in tables_list]
    return schema, rows

def execute_describe_table(ir):
    table_name = ir["name"]
    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' not found.")
    return tables[table_name]["columns"], []

def execute_transaction(ir):
    action = ir["action"]
    if action == "begin":
        # transaction_begin()
        pass
    elif action == "commit":
        # transaction_commit()
        pass
    elif action == "rollback":
        # transaction_rollback()
        pass
    else:
        raise ValueError(f"Unknown transaction action: {action}")
    return {"status": "OK", "transaction": action}


def execute_query(ir):
    qtype = ir["type"]
    if qtype == "select":
        return execute_select(ir)
    elif qtype == "insert":
        return execute_insert(ir)
    elif qtype == "update":
        return execute_update(ir)
    elif qtype == "delete":
        return execute_delete(ir)
    elif qtype == "create_table":
        return execute_create_table(ir)
    elif qtype == "drop_table":
        return execute_drop_table(ir)
    elif qtype == "rename_table":
        return execute_rename_table(ir)
    # elif qtype == "create_view":
    #     return execute_create_view(ir)
    elif qtype == "show_table":
        return execute_show_table(ir)
    elif qtype == "describe_table":
        return execute_describe_table(ir)
    elif qtype == "transaction":
        return execute_transaction(ir)
    else:
        raise NotImplementedError(f"Query type '{qtype}' not supported.")