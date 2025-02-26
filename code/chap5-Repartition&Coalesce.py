from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import col



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

# get partition numbers
print(df.rdd.getNumPartitions())

# repartitioning,is  used to increase or reduce the number of partitions with shuffling
df = df.repartition(5)
print(df.rdd.getNumPartitions())

# repartitioning based on the column
df = df.repartition(col("DEST_COUNTRY_NAME"))
print(df.rdd.getNumPartitions())

# repartitioning based on the number of partitions and a column
df = df.repartition(5, col("DEST_COUNTRY_NAME"))
print(df.rdd.getNumPartitions())

# coalesce, isused to reduce the partitions without shuffling
df = df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)
print(df.rdd.getNumPartitions())

