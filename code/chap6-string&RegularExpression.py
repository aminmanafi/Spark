from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower, upper
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import translate
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import instr
from pyspark.sql.functions import expr, locate




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

# capitaliza the first letter
df.select(initcap(col("Description"))).show()

# upper case & lower case
df.select(col("Description"),
lower(col("Description")),
upper(lower(col("Description")))).show(2)

# add or remove space around the strings
df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)


# regular expression
regex_string = "BLACK|WHITE|RED|GREEN|BLUE" # Words to be replaced
df.select(
regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
col("Description")).show(2)

# replace in character level
# L -> 1, E -> 3, T -> 7
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\
.show(2)

# extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
col("Description")).show(5)

# contain or instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
.where("hasSimpleColor")\
.select("Description").show(3, False)

# dynamic args - list of args
simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
    return locate(color_string.upper(), column)\
    .cast("boolean")\
    .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type
df.select(*selectedColumns).where(expr("is_white OR is_red"))\
.select("Description").show(3, False)

