from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F


def perform_operations(sqlcontext):
    """
    Function to read csv file and perform some operation on dataframe
    """

    # Read all files
    student_details = sqlcontext.read.csv('/home/chai/student_details.csv', header='true')
    students = sqlcontext.read.csv('/home/chai/students.csv', header='true')
    departments = sqlcontext.read.csv('/home/chai/departments.csv', header='true')

    # Join students, department and student_details
    students_details_all = student_details.\
        join(students, students.id == student_details.student_id).\
        join(departments, departments.id == student_details.dept_id)

    # Get number of students per dept
    groupby_dept_df = students_details_all.groupBy(students_details_all.dept_name).agg(
        F.countDistinct(students_details_all.student_id).alias('number_of_students'))

    # Calculate percentage of marks for each student
    percentage_df = students_details_all.groupBy(students_details_all.student_id).agg(
        F.round((F.sum(students_details_all.marks) / (F.countDistinct(students_details_all.subject_id)))).alias(
            'percentage'))

    groupby_dept_df.show()
    percentage_df.show()


def main():
    spark = SparkSession.builder.\
        master('local').appName('test').\
        config('spark.executor.memory', '1gb').\
        config('spark.cores.max', '2').\
        getOrCreate()
    sc = spark.sparkContext
    sqlcontext = SQLContext(sc)

    # Read parquet file
    _= perform_operations(sqlcontext)


if __name__ == '__main__':
    main()

