from pyspark.sql import SparkSession
from pyspark.sql.functions import max
from pyspark.sql.functions import desc

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Basic") \
    .getOrCreate()


flightData2015 = spark\
.read\
.option("inferSchema", "true")\
.option("header", "true")\
.csv("../data/flight-data/csv/2015-summary.csv")


# create a sql view of the DF
flightData2015.createOrReplaceTempView("flight_data_2015")

# run sql query => max agg
sqlWay = spark.sql("SELECT max(count) from flight_data_2015").take(1)

# the same using spark commands
dataFrameWay = flightData2015.select(max("count")).take(1)


print(sqlWay)
print(dataFrameWay)

# run sql query => sum agg
sqlWay = maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

# the same using spark commands
dataFrameWay = flightData2015\
.groupBy("DEST_COUNTRY_NAME")\
.sum("count")\
.withColumnRenamed("sum(count)", "destination_total")\
.sort(desc("destination_total"))\
.limit(5)

sqlWay.show()
dataFrameWay.show()