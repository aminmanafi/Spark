from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col
from time import sleep

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
.load("../data/retail-data/by-day/2010-12-01.csv")


# create a sql temp table or view 
staticDataFrame.createOrReplaceTempView("retail_data")

# store DF's schema
staticSchema = staticDataFrame.schema

# Stream Reading
# to read multiple files send the directory address
# maxFilesPerTrigger simply specifies the number of files we should read in at once
streamingDataFrame = spark.readStream\
.schema(staticSchema)\
.option("maxFilesPerTrigger", 1)\
.format("csv")\
.option("header", "true")\
.load("../data/retail-data/by-day/*.csv")



# selectExpr Allows you to select columns using SQL expressions.
# window Bucketize rows into one or more time windows given a timestamp specifying column.
purchaseByCustomerPerHour = streamingDataFrame\
.selectExpr(
"CustomerId",
"(UnitPrice * Quantity) as total_cost",
"InvoiceDate")\
.groupBy(
col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
.sum("total_cost")

# mutate the data in the in-memory table
writestreaming = purchaseByCustomerPerHour.writeStream\
.format("memory")\
.queryName("customer_purchases")\
.outputMode("complete")\
.start()

#writestreaming.awaitTermination()  # If you want to continue the execution
sleep(10)
writestreaming.stop()


spark.sql("""
SELECT *
FROM customer_purchases
ORDER BY 'sum(total_cost)' DESC
""")\
.show()