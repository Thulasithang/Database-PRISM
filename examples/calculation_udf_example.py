from core.sqlite_table import SQLiteTable
from typing import List, Dict, Any
import sqlite3

def calculate_weighted_gpa(grades: List[float], weights: List[int]) -> float:
    """
    Calculate weighted GPA from a list of grades and weights.
    This function will be registered as a UDF to handle aggregate calculations.
    """
    if not grades or not weights or len(grades) != len(weights):
        return 0.0
    
    weighted_sum = sum(grade * weight for grade, weight in zip(grades, weights))
    total_weights = sum(weights)
    return round(weighted_sum / total_weights, 2) if total_weights > 0 else 0.0

class GradeCalculator:
    def __init__(self, table: SQLiteTable):
        self.table = table
        self._setup_udfs()
    
    def _setup_udfs(self):
        """Register all UDFs needed for grade calculations."""
        # Register the weighted GPA calculator
        self.table.register_udf("calculate_gpa", 2, calculate_weighted_gpa)
    
    def get_student_gpas(self) -> List[Dict[str, Any]]:
        """Get GPAs for all students using the registered UDF."""
        return self.table.execute_query("""
            WITH GradeArrays AS (
                SELECT 
                    student_id,
                    GROUP_CONCAT(grade) as grades,
                    GROUP_CONCAT(credits) as weights
                FROM student_grades
                GROUP BY student_id
            )
            SELECT 
                student_id,
                calculate_gpa(
                    grades,
                    weights
                ) as weighted_gpa
            FROM GradeArrays
            ORDER BY student_id
        """)

def main():
    # Create and set up the grades table
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

    # Create calculator instance
    calculator = GradeCalculator(grades_table)
    
    # Get GPAs using the UDF
    results = calculator.get_student_gpas()
    
    # Print results
    print("\nStudent Weighted GPAs (Using UDF):")
    print("================================")
    for row in results:
        print(f"Student {row['student_id']}:")
        print(f"  Weighted GPA: {row['weighted_gpa']}")
        print("---------------------")

    # Show how the same UDF can be used in a different query context
    print("\nDetailed Analysis (Using same UDF):")
    print("================================")
    detailed_analysis = grades_table.execute_query("""
        WITH StudentSubjectGroups AS (
            SELECT 
                student_id,
                subject,
                GROUP_CONCAT(grade) as subject_grades,
                GROUP_CONCAT(credits) as subject_credits
            FROM student_grades
            GROUP BY student_id, subject
        )
        SELECT 
            student_id,
            subject,
            calculate_gpa(
                subject_grades,
                subject_credits
            ) as subject_gpa
        FROM StudentSubjectGroups
        ORDER BY student_id, subject
    """)
    
    for row in detailed_analysis:
        print(f"Student {row['student_id']} - {row['subject']}:")
        print(f"  Subject GPA: {row['subject_gpa']}")
        print("---------------------")

if __name__ == "__main__":
    main() 