from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp
from pyspark.sql.functions import col, date_add, date_sub
from pyspark.sql.functions import lit, datediff, months_between, to_date, to_timestamp







# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Intermediate") \
    .getOrCreate()



# dataframe, date and timestamp
dateDF = spark.range(10)\
.withColumn("today", current_date())\
.withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")
dateDF.printSchema()

dateDF.show(10, False)

# add- subtract
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

# data differences, days
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
.select(datediff(col("week_ago"), col("today"))).show(1)

# data differences, months
dateDF.select(
to_date(lit("2016-01-01")).alias("start"),
to_date(lit("2017-05-22")).alias("end"))\
.select(months_between(col("start"), col("end"))).show(1)

# convert string to date using to_date
spark.range(5).withColumn("date", lit("2017-01-01"))\
.select(to_date(col("date"))).show(1)

# no error, return null
dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

# use to_date with format
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")
cleanDateDF.show()

# use to_timestamp with format
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

# comparing
cleanDateDF.filter(col("date2") > lit("2017-12-12")).show()
cleanDateDF.filter(col("date2") > "'2017-12-12'").show()
