# Example usage of Database-PRISM with SQLite

from core.sqlite_table import SQLiteTable

# 1. Create a new table
employees = SQLiteTable(
    name="employees",
    columns=["id", "name", "department", "salary"],
    col_types={
        "id": "INTEGER",
        "name": "TEXT",
        "department": "TEXT",
        "salary": "REAL"
    }
)

# 2. Insert some data
employees.insert([1, "John Doe", "Engineering", 75000.0])
employees.insert([2, "Jane Smith", "Marketing", 65000.0])
employees.insert([3, "Bob Wilson", "Engineering", 70000.0])

# 3. Different ways to work with the data:

# a) Get all employees
all_employees = employees.select_all()
print("All employees:", all_employees)

# b) Custom query - Find engineers
engineers = employees.execute_query(
    "SELECT * FROM employees WHERE department = ?",
    ("Engineering",)
)
print("Engineers:", engineers)

# c) Update salary
employees.update(
    set_values={"salary": 80000.0},
    where_clause="name = ?",
    params=("John Doe",)
)

# d) Complex query - Average salary by department
avg_salary = employees.execute_query("""
    SELECT department, AVG(salary) as avg_salary 
    FROM employees 
    GROUP BY department
""")
print("Average salaries:", avg_salary)

# e) Delete an employee
employees.delete(
    where_clause="name = ?",
    params=("Bob Wilson",)
)