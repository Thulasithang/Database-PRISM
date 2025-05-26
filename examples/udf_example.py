from core.sqlite_table import SQLiteTable

def weighted_average(grade: float, weight: int) -> float:
    """Calculate weighted average of a grade."""
    return grade * weight

def main():
    # Create a table for student grades
    grades_table = SQLiteTable(
        name="student_grades",
        columns=["student_id", "subject", "grade", "credits"],
        col_types={
            "student_id": "INTEGER",
            "subject": "TEXT",
            "grade": "REAL",
            "credits": "INTEGER"
        }
    )

    # Insert sample data
    sample_data = [
        [1, "Math", 85.0, 4],
        [1, "Physics", 90.0, 3],
        [1, "Chemistry", 88.0, 3],
        [2, "Math", 92.0, 4],
        [2, "Physics", 85.0, 3],
        [2, "Chemistry", 95.0, 3]
    ]
    
    for row in sample_data:
        grades_table.insert(row)

    # Register the UDF with our table
    grades_table.register_udf("weighted_average", 2, weighted_average)

    # Use the UDF to calculate weighted GPA
    result = grades_table.execute_query("""
        SELECT 
            student_id,
            ROUND(
                SUM(weighted_average(grade, credits)) * 1.0 / SUM(credits),
                2
            ) as weighted_gpa,
            SUM(credits) as total_credits
        FROM student_grades
        GROUP BY student_id
        ORDER BY student_id
    """)

    # Print results
    print("\nStudent Weighted GPAs:")
    print("=====================")
    for row in result:
        print(f"Student {row['student_id']}:")
        print(f"  Weighted GPA: {row['weighted_gpa']}")
        print(f"  Total Credits: {row['total_credits']}")
        print("---------------------")

    # Show individual grades for verification
    details = grades_table.execute_query("""
        SELECT 
            student_id,
            subject,
            grade,
            credits
        FROM student_grades
        ORDER BY student_id, subject
    """)

    print("\nDetailed Grades:")
    print("===============")
    for row in details:
        print(f"Student {row['student_id']} - {row['subject']}:")
        print(f"  Grade: {row['grade']}")
        print(f"  Credits: {row['credits']}")
        print("---------------------")

if __name__ == "__main__":
    main() 