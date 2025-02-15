from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Basic") \
    .getOrCreate()


# create a range 0-1000 and convert it to DataFrame
myRange = spark.range(1000).toDF("number")

# define condition
divisBy2 = myRange.where("number % 2 = 0")

# compute count of data in DF
print(divisBy2.count())