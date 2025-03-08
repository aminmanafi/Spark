from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import col, expr, to_date, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, max, dense_rank, rank
from pyspark.sql.functions import sum, grouping_id



# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Intermediate") \
    .getOrCreate()


# reading the data
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("../data/retail-data/all/*.csv")\
.coalesce(5)
df.cache() # optimization technique, keeps df in memory
df.createOrReplaceTempView("dfTable")

# grouping
df.groupBy("InvoiceNo", "CustomerId").count().show()

# grouping with expressions
df.groupBy("InvoiceNo").agg(
count("Quantity").alias("quan"),
expr("count(Quantity)")).show()

# grouping with maps
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
.show()

# create date column
dfWithTimestamp = df.withColumn("timestamp", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
dfWithDate = dfWithTimestamp.withColumn("date", to_date(col("timestamp")))
dfWithDate.createOrReplaceTempView("dfWithDate")

# create window function
windowSpec = Window\
.partitionBy("CustomerId", "date")\
.orderBy(desc("Quantity"))\
.rowsBetween(Window.unboundedPreceding, Window.currentRow)

# agg
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

# ranking
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)


dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()


## grouping sets
dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")


# rollup
rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
.selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
.orderBy("Date")
rolledUpDF.show()

# cube
dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
.select("Date", "Country", "sum(Quantity)").orderBy("Date").show()

# grouping metadata
dfNoNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))\
.orderBy(expr("grouping_id()").desc)\
.show()

# pivot
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()
