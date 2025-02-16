from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Basic") \
    .getOrCreate()

# set number of partition to 5 in local machines
spark.conf.set("spark.sql.shuffle.partitions", "5")

# to read multiple files send the directory address
staticDataFrame = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("../data/retail-data/by-day/*.csv")

# create a sql temp table or view 
staticDataFrame.createOrReplaceTempView("retail_data")

# store DF's schema
staticSchema = staticDataFrame.schema

# selectExpr Allows you to select columns using SQL expressions.
# window Bucketize rows into one or more time windows given a timestamp specifying column.
staticDataFrame\
.selectExpr(
"CustomerId",
"(UnitPrice * Quantity) as total_cost",
"InvoiceDate")\
.groupBy(
col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
.sum("total_cost")\
.show()
