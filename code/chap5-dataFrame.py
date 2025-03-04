from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import expr, col, column
from pyspark.sql.functions import lit


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Intermediate") \
    .getOrCreate()

# defining schema
myManualSchema = StructType([
StructField("DEST_COUNTRY_NAME", StringType(), True),
StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
StructField("count", LongType(), False, metadata={"hello":"world"})
])

# read json file with schema
df = spark.read.format("json").schema(myManualSchema)\
.load("../data/flight-data/json/2015-summary.json")

# create temp view
df.createOrReplaceTempView("dfTable")

# select
df.select("DEST_COUNTRY_NAME").show(2)
# in SQL:
# SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2


df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

# reffer to column with diffrenet ways
df.select(
expr("DEST_COUNTRY_NAME AS destinationA"),
col("DEST_COUNTRY_NAME").alias("destinationB"),
column("DEST_COUNTRY_NAME"))\
.show(2)

# shorthand for using select with expr
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)


df.selectExpr(
"*", # all original columns
"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
.show(2)

# agg with selectExpr
df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)



df.select(expr("*"), lit(1).alias("One")).show(2)