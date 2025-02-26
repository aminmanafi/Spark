from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import expr, col, column
from pyspark.sql import Row
from pyspark.sql.functions import desc, asc, asc_nulls_first, desc_nulls_first, asc_nulls_last, desc_nulls_last




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

# filter or where, both are the same
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)

# multiple filters
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
.show(2)

# unique or distinct
print(df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count())
print(df.select("ORIGIN_COUNTRY_NAME").distinct().count())

# random samples
seed = 5
withReplacement = False
fraction = 0.5
print(df.sample(withReplacement, fraction, seed).count())

# random split
dataFrames = df.randomSplit([0.25, 0.75], seed)
print(dataFrames[0].count() > dataFrames[1].count())


# Concatenating and Appending Rows (Union)
schema = df.schema
newRows = [
Row("New Country", "Other Country", 5),
Row("New Country 2", "Other Country 3", 1)
]

parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)
# in Python
df.union(newDF)\
.where("count = 1")\
.where(col("ORIGIN_COUNTRY_NAME") != "United States")\
.show()

# sorting, sort or order by
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

# sorting desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# specify where nulls to be shown
df.orderBy(col("count").desc_nulls_first(), col("DEST_COUNTRY_NAME").asc_nulls_first()).show(2)
df.orderBy(col("count").desc_nulls_last(), col("DEST_COUNTRY_NAME").asc_nulls_last()).show(2)

# sort within partitions
spark.read.format("json").load("../data/flight-data/json/*-summary.json")\
.sortWithinPartitions("count")

# limit
df.limit(5).show()
df.orderBy(expr("count desc")).limit(6).show()

# Collecting Rows to the Driver
collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count, first 5 rows
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect() # get all data
