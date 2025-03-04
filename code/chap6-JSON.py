from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, json_tuple, to_json
from pyspark.sql.functions import from_json
from pyspark.sql.types import *



# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Intermediate") \
    .getOrCreate()

# read the data
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("../data/retail-data/by-day/2010-12-01.csv")

df.printSchema()
df.createOrReplaceTempView("dfTable")



# create json row
jsonDF = spark.range(1).selectExpr("""
'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

# get json object
jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]").alias("column"),
    json_tuple(col("jsonString"), "myJSONKey")).show(2)

# convert to json
df.selectExpr("(InvoiceNo, Description) as myStruct")\
.select(to_json(col("myStruct"))).show(5, False)

# from json
parseSchema = StructType((
StructField("InvoiceNo",StringType(),True),
StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo, Description) as myStruct")\
.select(to_json(col("myStruct")).alias("newJSON"))\
.select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)






