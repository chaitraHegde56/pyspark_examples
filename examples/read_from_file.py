from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


def read_parquet(sqlcontext):
    """
    Function to read parquet file and return dataframe
    """
    dataframe = sqlcontext.read.parquet('/home/chai/test.parquet')
    return dataframe


def main():
    spark = SparkSession.builder.\
        master('local').appName('test').\
        config('spark.executor.memory', '1gb').\
        config('spark.cores.max', '2').\
        getOrCreate()
    sc = spark.sparkContext
    sqlcontext = SQLContext(sc)
    dataframe = read_parquet(sqlcontext)
    dataframe.show()


if __name__ == '__main__':
    main()

