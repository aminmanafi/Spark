from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.functions import split, size, array_contains, explode, create_map






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


# struct
df.selectExpr("(Description, InvoiceNo) as complex", "*").show()
df.selectExpr("struct(Description, InvoiceNo) as complex", "*").show()

complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")
complexDF.show()

# select from struct
complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description")).show()

# select all values
complexDF.select("complex.*").show()


# array
# make it using split
df.select(split(col("Description"), " ")).show(2, False)

# query the values
df.select(split(col("Description"), " ").alias("array_col"))\
.selectExpr("array_col[0]").show(2)

# lenght
df.select(size(split(col("Description"), " "))).show(2)

# contain
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

# explode
df.withColumn("splitted", split(col("Description"), " "))\
.withColumn("exploded", explode(col("splitted")))\
.select("Description", "InvoiceNo", "exploded").show(20, False)

# map
df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.show(2, False)

df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.selectExpr("complex_map['WHITE METAL LANTERN']").show(5, False)

df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.selectExpr("explode(complex_map)").show(20, False)