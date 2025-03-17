from pyspark.sql import SparkSession

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

# run sql query
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

# the same using spark commands
dataFrameWay = flightData2015\
.groupBy("DEST_COUNTRY_NAME")\
.count()


sqlWay.show()
dataFrameWay.show()