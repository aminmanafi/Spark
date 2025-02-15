from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Basic") \
    .getOrCreate()

# read csv file, set header true when the files have a header
flightData2015 = spark\
.read\
.option("inferSchema", "true")\
.option("header", "true")\
.csv("../data/flight-data/csv/2015-summary.csv")

# sort based on the 'count' column
sortedFlightData2015 = flightData2015.sort("count")

sortedFlightData2015.show()