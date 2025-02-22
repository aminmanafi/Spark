from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import expr, col, column
from pyspark.sql.functions import lit


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Basic") \
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

# add new literal column
df.withColumn("numberOne", lit(1)).show(2)


df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
.show(2)

# renaming, keeps both columns
df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns
# result: DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count, Destination

#renaming, omits first column
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
# result: dest, ORIGIN_COUNTRY_NAME, count

# dont need escape
# the first argument to withColumn is just a string for the new column name.
dfWithLongColName = df.withColumn(
"This Long Column-Name",
expr("ORIGIN_COUNTRY_NAME"))

# escpae needed
# we need to use backticks because we’re referencing a column in an expression
# we have to use `
dfWithLongColName.selectExpr(
"`This Long Column-Name`",
"`This Long Column-Name` as `new col`")\
.show(2)
dfWithLongColName.createOrReplaceTempView("dfTableLong")
# -- in SQL
# SELECT `This Long Column-Name`, `This Long Column-Name` as `new col`
# FROM dfTableLong LIMIT 2

dfWithLongColName.select(expr("`This Long Column-Name`")).columns

# drop column
df.drop("ORIGIN_COUNTRY_NAME")

# drop multiple column
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

# convert from integer type to long
df.withColumn("count2", col("count").cast("long"))
# in SQL
# SELECT *, cast(count as long) AS count2 FROM dfTable

