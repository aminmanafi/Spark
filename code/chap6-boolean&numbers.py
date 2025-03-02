from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.functions import lit, instr, expr, pow, round, bround, corr, monotonically_increasing_id




# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Intermediate") \
    .getOrCreate()

df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("../data/retail-data/by-day/2010-12-01.csv")

df.printSchema()
df.createOrReplaceTempView("dfTable")

# converting to spark type using lit
df.select(lit(5), lit("five"), lit(5.0))

# boolean type
from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
.select("InvoiceNo", "Description")\
.show(5, False)

df.where("InvoiceNo = 536365")\
.show(5, False)
df.where("InvoiceNo <> 536365")\
.show(5, False)

# or-and statement
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

# boolean column, isExpensive is a boolean column
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)

# boolean and where clause
df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))\
.where("isExpensive")\
.select("Description", "UnitPrice").show(5)

# numbers
# pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

df.selectExpr(
"CustomerId",
"(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

# round
df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

# correlation
print(df.stat.corr("Quantity", "UnitPrice"))
df.select(corr("Quantity", "UnitPrice")).show()

# summary statistics
df.describe().show()

# we can use each of these statistics separately
from pyspark.sql.functions import count, mean, stddev_pop, min, max

# approximate quantiles
colName = "UnitPrice"
quantileProbs = [0.5]
relError = 0.05
print(df.stat.approxQuantile(colName, quantileProbs, relError))

# cross-tabulation or 
df.stat.crosstab("StockCode", "Quantity").show()

# frequent
df.stat.freqItems(["StockCode", "Quantity"]).show()

# add unique id to each row
df.select(monotonically_increasing_id()).show(2)






