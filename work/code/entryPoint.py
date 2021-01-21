from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def testFunction():
    print('Hello from function')
