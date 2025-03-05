from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, approx_count_distinct
from pyspark.sql.functions import first, last, min, max, sum, sumDistinct
from pyspark.sql.functions import sum, count, avg, expr
from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
from pyspark.sql.functions import skewness, kurtosis
from pyspark.sql.functions import corr, covar_pop, covar_samp
from pyspark.sql.functions import collect_set, collect_list




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

# can you as transformation or an action
df.count()


# count of a column or all
df.select(count("StockCode")).show()
df.select(count("*")).show()

# count distinct
df.select(countDistinct("StockCode")).show()

# approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show()

# first-last
df.select(first("StockCode"), last("StockCode")).show()

# min, max
df.select(min("Quantity"), max("Quantity")).show()

# sum
df.select(sum("Quantity")).show()

# sumDistinct
df.select(sumDistinct("Quantity")).show()

# average 3 method
df.select(
count("Quantity").alias("total_transactions"),
sum("Quantity").alias("total_purchases"),
avg("Quantity").alias("avg_purchases"),
expr("mean(Quantity)").alias("mean_purchases"))\
.selectExpr(
"total_purchases/total_transactions",
"avg_purchases",
"mean_purchases").show()

# variance & standard deviation
df.select(var_pop("Quantity"), var_samp("Quantity"),
stddev_pop("Quantity"), stddev_samp("Quantity")).show()

# skewness & kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()

# covariance & correlation
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
covar_pop("InvoiceNo", "Quantity")).show()

# complex type
df.agg(collect_set("Country"), collect_list("Country")).show()



