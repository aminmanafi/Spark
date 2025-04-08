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
driver = "org.sqlite.JDBC"
path = "/data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"

