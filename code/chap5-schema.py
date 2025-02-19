from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import expr, col


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


# print schema
df.printSchema()
print(df.schema)

# columns
print(df.columns)

# print first row
print(df.first())


df.show()









