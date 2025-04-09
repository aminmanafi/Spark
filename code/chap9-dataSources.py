from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType



# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Intermediate") \
    .getOrCreate()



## CSV
# read
csvFile = spark.read.format("csv")\
               .option("header", "true")\
               .option("mode", "FAILFAST")\
               .option("inferSchema", "true")\
               .load("../data/flight-data/csv/2010-summary.csv")

# write
csvFile.write.format("csv")\
       .mode("overwrite")\
       .option("sep", ",")\
       .save("../tmp/my-tsv-file.tsv")

## JSON
# read
spark.read.format("json").option("mode", "FAILFAST")\
                         .option("inferSchema", "true")\
                         .load("/data/flight-data/json/2010-summary.json").show(5)

# write
csvFile.write.format("json")\
       .mode("overwrite")\
       .save("/tmp/my-json-file.json")

## PARQUET
# read
spark.read.format("parquet")\
     .load("/data/flight-data/parquet/2010-summary.parquet").show(5)

# write
csvFile.write.format("parquet")\
       .mode("overwrite")\
       .save("/tmp/my-parquet-file.parquet")

## ORC
# read
spark.read.format("orc")\
     .load("/data/flight-data/orc/2010-summary.orc").show(5)

# write
csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")

## SQL DATABASE
# read
# to conccet databases to spark you should add db connector jdbc
# file and then after spark-submit you should add --jars jdbc-path
driver = "org.sqlite.JDBC"
path = "/data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"

dbDataFrame = spark.read.format("jdbc").option("url", url)\
                   .option("dbtable", tablename)\
                   .option("driver", driver).load()

dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)

# read from a query
pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
AS flight_info"""
dbDataFrame = spark.read.format("jdbc")\
                   .option("url", url)\
                   .option("dbtable", pushdownQuery)\
                   .option("driver", driver)\
                   .load()

# parallel reading
pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
AS flight_info"""
dbDataFrame = spark.read.format("jdbc")\
                   .option("url", url)\
                   .option("dbtable", pushdownQuery)\
                   .option("driver", driver)\
                   .option("numPartitions", 10)\
                   .load()


# defining predicates
props = {"driver":"org.sqlite.JDBC"}
predicates = [
"DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
"DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"]

spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show()
print(spark.read.jdbc(url,tablename,predicates=predicates,properties=props)\
.rdd.getNumPartitions()) # 2

# predicates with duplicate rows
props = {"driver":"org.sqlite.JDBC"}
predicates = [
"DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
"DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).count() # 510

# partitioning based on sliding window
colName = "count"
lowerBound = 0
upperBound = 348113 # this is the max count in our database
numPartitions = 10

spark.read.jdbc(url, tablename, column=colName, properties=props,
              lowerBound=lowerBound, upperBound=upperBound,
              numPartitions=numPartitions).count() # 255

# write
newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)


