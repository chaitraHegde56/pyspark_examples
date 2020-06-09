from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


def read_parquet(sqlcontext):
    """
    Function to read parquet file and return dataframe
    """
    dataframe = sqlcontext.read.parquet('/home/chai/students.parquet')
    return dataframe


def read_csv(sqlcontext):
    """
    Function to read csv file and return dataframe
    """
    dataframesqlcontext.read.csv('/home/chai/students.csv', header='true')
    return dataframe


def main():
    spark = SparkSession.builder.\
        master('local').appName('test').\
        config('spark.executor.memory', '1gb').\
        config('spark.cores.max', '2').\
        getOrCreate()
    sc = spark.sparkContext
    sqlcontext = SQLContext(sc)

    # Read parquet file
    dataframe = read_parquet(sqlcontext)
    dataframe.show()

    # Read csv file
    dataframe = read_csv(sqlcontext)
    dataframe.show()


if __name__ == '__main__':
    main()

