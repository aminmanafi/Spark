from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.functions import ifnull, nullif, nvl, nvl2



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

# select first column with no null values
df.select(coalesce(col("Description"), col("CustomerId"))).show(20, False)

# treat with nulls
df.select(ifnull(lit(None), lit('return_value')),
nullif(lit('value'), lit('value')),
nvl(lit(None), lit('return_value')),
nvl2(lit('not_null'), lit('return_value'), lit("else_value"))).show(1)

# drop null rows
df.na.drop() # if any column of the row conatins null value
df.na.drop("any") # if any column of the row conatins null value
df.na.drop("all") # if all columns of the row conatin null values
df.na.drop("all", subset=["StockCode", "InvoiceNo"]) # set a column names

# fill
df.na.fill("All Null values become this string") # all string columns
df.na.fill("all", subset=["StockCode", "InvoiceNo"]) # set column names
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals) # set different fill value for each column

# replace
df.na.replace([""], ["UNKNOWN"], "Description")

